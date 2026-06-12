"""
Page Parser - Parses Canvas wiki pages.

Extracts page content from wiki_content/*.xml, wiki_content/*.html, and
web_resources/**/*.html files (all three locations Canvas uses for webcontent).
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from models.canvas_models import CanvasPage, WorkflowState
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, get_element_text
from utils.html_utils import sanitize_html, get_inner_html


class PageParser:
    """
    Parses Canvas page XML files and HTML pages from wiki_content/ and web_resources/.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize page parser.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.wiki_content_dir = course_directory / "wiki_content"
        self.web_resources_dir = course_directory / "web_resources"
        self.errors: List[MigrationError] = []
    
    def parse_page(self, page_file: Path) -> Optional[CanvasPage]:
        """
        Parse a single page XML file.
        
        Args:
            page_file: Path to page XML file
            
        Returns:
            CanvasPage object or None if parsing fails
        """
        try:
            root = parse_xml_file(page_file)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.ERROR,
                    error_type="PAGE_PARSE_ERROR",
                    message=f"Failed to parse page file: {page_file.name}",
                    file_path=str(page_file)
                ))
                return None
            
            # Extract page data
            title = self._extract_title(root, page_file)
            body = self._extract_body(root)
            workflow_state = self._extract_workflow_state(root)
            
            page = CanvasPage(
                title=title,
                identifier=page_file.stem,
                body=body,
                workflow_state=workflow_state,
                source_file=str(page_file)
            )
            
            return page
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.ERROR,
                error_type="PAGE_PARSE_ERROR",
                message=f"Unexpected error parsing page: {str(e)}",
                file_path=str(page_file)
            ))
            return None
    
    def _extract_title(self, root, page_file: Path) -> str:
        """Extract page title"""
        title_elem = find_element(root, './/title', {})
        if title_elem is not None:
            return get_element_text(title_elem, page_file.stem)
        return page_file.stem
    
    def _extract_body(self, root) -> str:
        """Extract page body HTML"""
        body_elem = find_element(root, './/body', {})
        if body_elem is not None:
            # Get inner HTML
            body_html = get_inner_html(body_elem)
            return sanitize_html(body_html)
        
        # Fallback: try text element
        text_elem = find_element(root, './/text', {})
        if text_elem is not None:
            return sanitize_html(get_element_text(text_elem, ""))
        
        return ""
    
    def _extract_workflow_state(self, root) -> WorkflowState:
        """Extract workflow state"""
        state_elem = find_element(root, './/workflow_state', {})
        if state_elem is not None:
            state_text = get_element_text(state_elem, "active").lower()
            if state_text == "unpublished":
                return WorkflowState.UNPUBLISHED
            elif state_text == "deleted":
                return WorkflowState.DELETED
        return WorkflowState.ACTIVE
    
    def parse_html_page(
        self,
        html_file: Path,
        identifier: Optional[str] = None,
    ) -> Optional[CanvasPage]:
        """Parse an HTML file directly as a page.

        Args:
            html_file:   Path to the .html file.
            identifier:  Resource identifier from the manifest.  When supplied
                         the page is keyed by this ID so the transformer can
                         look it up via ``_content_ref``.  Falls back to the
                         file stem when omitted (wiki_content files).
        """
        try:
            with open(html_file, 'r', encoding='utf-8', errors='replace') as f:
                raw = f.read()
            from utils.html_utils import sanitize_html, get_body_content
            body = get_body_content(raw) or sanitize_html(raw)
            title = html_file.stem.replace('-', ' ').replace('_', ' ').title()
            return CanvasPage(
                title=title,
                identifier=identifier or html_file.stem,
                body=body,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(html_file)
            )
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="PAGE_PARSE_ERROR",
                message=f"Failed to parse HTML page: {e}",
                file_path=str(html_file)
            ))
            return None

    def parse_all_pages(
        self,
        resource_href_map: Optional[Dict[str, str]] = None,
    ) -> List[CanvasPage]:
        """Parse all pages from wiki_content/ and web_resources/.

        Canvas exports place HTML content in two locations:

        * ``wiki_content/`` — wiki pages exported as ``.html`` or ``.xml``
        * ``web_resources/`` — files uploaded directly to Canvas modules
          (tutorials, templates, homework starters, etc.)

        Both are declared as ``webcontent`` resources in ``imsmanifest.xml``.
        Previously only ``wiki_content`` was scanned, causing every
        ``web_resources`` HTML item to appear as "No content" in the report.

        Args:
            resource_href_map: Mapping of ``href`` (relative to course root,
                as it appears in the manifest) → resource ``identifier``.
                When provided, each parsed page is keyed by its manifest
                resource ID so the transformer can resolve it via
                ``_content_ref``.  Built by ``Parser`` from the manifest
                resource map.
        """
        pages = []
        # Normalise the href map so lookups are case-insensitive and use
        # forward slashes regardless of OS path separator.
        href_to_res_id: Dict[str, str] = {}
        if resource_href_map:
            for href, res_id in resource_href_map.items():
                href_to_res_id[href.replace("\\", "/").lower()] = res_id

# ── wiki_content/ ────────────────────────────────────────────────────
        if self.wiki_content_dir.exists():
            for page_file in self.wiki_content_dir.glob("*.xml"):
                page = self.parse_page(page_file)
                if page:
                    pages.append(page)

            for page_file in self.wiki_content_dir.glob("*.html"):
                # Derive the manifest href for this file so we can look up
                # the resource ID (e.g. "wiki_content/module-2-overview.html")
                rel_href = page_file.relative_to(self.course_directory)
                rel_key  = str(rel_href).replace("\\", "/").lower()
                res_id   = href_to_res_id.get(rel_key)
                page = self.parse_html_page(page_file, identifier=res_id)
                if page:
                    pages.append(page)

        # ── web_resources/ ───────────────────────────────────────────────────
        # Recursively find every .html file.  Only import files that are
        # actually referenced in the manifest (present in href_to_res_id);
        # unreferenced files are media assets, not lesson pages.
        if self.web_resources_dir.exists():
            for html_file in self.web_resources_dir.rglob("*.html"):
                rel_href = html_file.relative_to(self.course_directory)
                rel_key  = str(rel_href).replace("\\", "/").lower()
                res_id   = href_to_res_id.get(rel_key)
                if res_id is None:
                    # Not referenced in the manifest — skip (it's an asset,
                    # not a standalone lesson page).
                    continue
                page = self.parse_html_page(html_file, identifier=res_id)
                if page:
                    pages.append(page)
            
            # Also process IPYNB and CSV files referenced in manifest
            for ext, parse_func in [(".ipynb", self._parse_ipynb), (".csv", self._parse_csv)]:
                for resource_file in self.web_resources_dir.rglob(f"*{ext}"):
                    rel_href = resource_file.relative_to(self.course_directory)
                    rel_key  = str(rel_href).replace("\\", "/").lower()
                    res_id   = href_to_res_id.get(rel_key)
                    if res_id is None:
                        continue
                    page = parse_func(resource_file, identifier=res_id)
                    if page:
                        pages.append(page)

        return pages
    
    def _parse_ipynb(self, ipynb_file: Path, identifier: str) -> Optional[CanvasPage]:
        """Extract content from Jupyter notebook (.ipynb)."""
        try:
            import json
            with open(ipynb_file, 'r', encoding='utf-8') as f:
                nb = json.load(f)
            
            cells = nb.get('cells', [])
            content_parts = []
            for cell in cells:
                cell_type = cell.get('cell_type', '')
                source = cell.get('source', [])
                if isinstance(source, list):
                    source = ''.join(source)
                if source.strip():
                    if cell_type == 'markdown':
                        # Convert markdown to HTML (basic)
                        from utils.html_utils import basic_markdown_to_html
                        content_parts.append(basic_markdown_to_html(source))
                    elif cell_type == 'code':
                        content_parts.append(f"<pre><code>{source}</code></pre>")
                    else:
                        content_parts.append(f"<p>{source}</p>")
            
            body = '\n'.join(content_parts)
            if len(body.strip()) < 10:
                return None
            
            title = nb.get('metadata', {}).get('name', ipynb_file.stem)
            return CanvasPage(
                title=title,
                identifier=identifier,
                body=body,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(ipynb_file)
            )
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="IPYNB_PARSE_ERROR",
                message=f"Failed to parse notebook: {str(e)}",
                file_path=str(ipynb_file),
                suggested_action="File will be skipped"
            ))
            return None
    
    def _parse_pdf(self, pdf_file: Path, identifier: str) -> Optional[CanvasPage]:
        """Extract text content from PDF file."""
        try:
            import PyPDF2
            from utils.html_utils import sanitize_html
            
            with open(pdf_file, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text_parts = []
                for page in reader.pages[:10]:  # Limit to first 10 pages
                    text = page.extract_text()
                    if text:
                        text_parts.append(f"<p>{text}</p>")
            
            body = '\n'.join(text_parts)
            if len(body.strip()) < 10:
                return None
            
            return CanvasPage(
                title=pdf_file.stem,
                identifier=identifier,
                body=sanitize_html(body),
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(pdf_file)
            )
        except ImportError:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.INFO,
                error_type="PDF_NO_PARSER",
                message="PyPDF2 not installed - cannot extract PDF content",
                file_path=str(pdf_file),
                suggested_action="pip install PyPDF2"
            ))
            return None
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="PDF_PARSE_ERROR",
                message=f"Failed to parse PDF: {str(e)}",
                file_path=str(pdf_file),
                suggested_action="File will be skipped"
            ))
            return None
    
    def _parse_csv(self, csv_file: Path, identifier: str) -> Optional[CanvasPage]:
        """Extract preview from CSV file."""
        try:
            import csv
            from utils.html_utils import sanitize_html
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)[:20]  # Limit to first 20 rows
            
            if not rows:
                return None
            
            rows_html = []
            for row in rows:
                cells = ''.join(f"<td>{sanitize_html(cell)}</td>" for cell in row)
                rows_html.append(f"<tr>{cells}</tr>")
            
            body = f"<table>{''.join(rows_html)}</table>"
            return CanvasPage(
                title=csv_file.stem,
                identifier=identifier,
                body=body,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(csv_file)
            )
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="CSV_PARSE_ERROR",
                message=f"Failed to parse CSV: {str(e)}",
                file_path=str(csv_file),
                suggested_action="File will be skipped"
            ))
            return None
