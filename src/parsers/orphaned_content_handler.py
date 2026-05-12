"""
Orphaned Content Handler - Handles XML files not referenced in manifest.

This includes PowerPoint XML exports, loose HTML files, and other orphaned content.
"""

from pathlib import Path
from typing import List, Optional
from lxml import etree

from models.canvas_models import CanvasPage, WorkflowState
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, get_element_text, get_inner_html
from utils.html_utils import sanitize_html
from utils.file_utils import is_xml_file, is_html_file
from config.canvas_schemas import SYSTEM_XML_FILES
from .pptx_parser import PptxParser


class OrphanedContentHandler:
    """
    Handles orphaned content (files not referenced in manifest).
    
    Supports:
    - PowerPoint XML exports
    - Loose HTML files
    - Loose PPTX files
    - Other XML content files
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize orphaned content handler.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.pptx_parser = PptxParser(course_directory)
        self.errors: List[MigrationError] = []
    
    def find_orphaned_xml_files(self, referenced_files: set) -> List[Path]:
        """
        Find XML files not referenced in manifest.
        
        Args:
            referenced_files: Set of files referenced in manifest
            
        Returns:
            List of orphaned XML file paths
        """
        orphaned = []
        
        # Search entire directory
        for xml_file in self.course_directory.rglob("*.xml"):
            # Skip system files
            if xml_file.name in SYSTEM_XML_FILES:
                continue
            
            # Skip output directory
            if 'tutor_lms_output' in str(xml_file):
                continue
            
            # Get relative path
            try:
                rel_path = str(xml_file.relative_to(self.course_directory))
            except ValueError:
                continue
            
            # Normalise separators so Windows backslashes match forward-slash hrefs.
            rel_path_norm = rel_path.replace('\\', '/')
            # Check if referenced
            if rel_path not in referenced_files and rel_path_norm not in referenced_files:
                orphaned.append(xml_file)
        
        return orphaned
    
    def parse_orphaned_xml(self, xml_file: Path) -> Optional[CanvasPage]:
        """
        Parse an orphaned XML file and convert to CanvasPage.
        
        Handles:
        - PowerPoint XML exports
        - Generic XML content
        - HTML embedded in XML
        
        Args:
            xml_file: Path to orphaned XML file
            
        Returns:
            CanvasPage object or None if parsing fails
        """
        try:
            root = parse_xml_file(xml_file)
            if root is None:
                return None
            
            # Extract title
            title = self._extract_title_from_xml(root, xml_file)
            
            # Extract content
            content = self._extract_content_from_xml(root)
            
            # If no content found, skip
            if not content or len(content.strip()) < 10:
                return None
            
            # Create page
            page = CanvasPage(
                title=title,
                identifier=f"orphaned_{xml_file.stem}",
                body=content,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(xml_file)
            )
            
            return page
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="ORPHANED_XML_PARSE_ERROR",
                message=f"Failed to parse orphaned XML: {str(e)}",
                file_path=str(xml_file),
                suggested_action="File will be skipped"
            ))
            return None
    
    def _extract_title_from_xml(self, root, xml_file: Path) -> str:
        """
        Extract title from XML file.
        
        Tries multiple common title elements.
        """
        # Try common title elements
        title_paths = [
            './/title',
            './/h1',
            './/heading',
            './/name',
            './/slide-title',
            './/presentation-title'
        ]
        
        for path in title_paths:
            elem = find_element(root, path, {})
            if elem is not None:
                title = get_element_text(elem, '')
                if title:
                    return title
        
        # Fallback to filename
        return xml_file.stem.replace('_', ' ').replace('-', ' ').title()
    
    def _extract_content_from_xml(self, root) -> str:
        """
        Extract content from XML file.
        
        Handles PowerPoint XML and generic XML content.
        """
        content_parts = []
        
        # Try common content elements
        content_paths = [
            './/body',
            './/content',
            './/text',
            './/description',
            './/slide-content',
            './/notes',
            './/p',  # Paragraphs
        ]
        
        for path in content_paths:
            elements = root.findall(path)
            for elem in elements:
                # Get inner HTML
                try:
                    html = get_inner_html(elem)
                    if html:
                        content_parts.append(html)
                except:
                    # Fallback to text
                    text = get_element_text(elem, '')
                    if text:
                        content_parts.append(f"<p>{text}</p>")
        
        # If no structured content found, try to extract all text
        if not content_parts:
            try:
                # Get all text content
                text = etree.tostring(root, encoding='unicode', method='text')
                if text and len(text.strip()) > 10:
                    # Wrap in paragraph
                    content_parts.append(f"<p>{text.strip()}</p>")
            except:
                pass
        
        # Combine and clean
        combined = '\n'.join(content_parts)
        return sanitize_html(combined)
    
    def parse_orphaned_html(self, html_file: Path) -> Optional[CanvasPage]:
        """
        Parse an orphaned HTML file.
        
        Args:
            html_file: Path to HTML file
            
        Returns:
            CanvasPage object or None
        """
        try:
            # Read HTML file
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract title from filename
            title = html_file.stem.replace('_', ' ').replace('-', ' ').title()
            
            # Create page
            page = CanvasPage(
                title=title,
                identifier=f"orphaned_{html_file.stem}",
                body=sanitize_html(content),
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(html_file)
            )
            
            return page
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="ORPHANED_HTML_PARSE_ERROR",
                message=f"Failed to parse orphaned HTML: {str(e)}",
                file_path=str(html_file),
                suggested_action="File will be skipped"
            ))
            return None
    
    def process_all_orphaned_content(
        self,
        referenced_files: set
    ) -> List[CanvasPage]:
        """
        Process all orphaned content files.
        
        Args:
            referenced_files: Set of files referenced in manifest
            
        Returns:
            List of CanvasPage objects created from orphaned content
        """
        pages = []
        
        # Find and process orphaned XML files
        orphaned_xml = self.find_orphaned_xml_files(referenced_files)
        
        print(f"  Found {len(orphaned_xml)} orphaned XML files")
        
        for xml_file in orphaned_xml:
            page = self.parse_orphaned_xml(xml_file)
            if page:
                pages.append(page)
                print(f"  [OK] Converted orphaned XML: {xml_file.name}")
        
        # Find orphaned HTML files
        for html_file in self.course_directory.rglob("*.html"):
            if 'tutor_lms_output' in str(html_file):
                continue
            
            try:
                rel_path = str(html_file.relative_to(self.course_directory))
            except ValueError:
                continue
            
            # Normalise separators so Windows backslashes match the forward-slash
            # hrefs stored in referenced_files (which come from the manifest).
            rel_path_norm = rel_path.replace('\\', '/')
            if rel_path not in referenced_files and rel_path_norm not in referenced_files:
                page = self.parse_orphaned_html(html_file)
                if page:
                    pages.append(page)
                    print(f"  [OK] Converted orphaned HTML: {html_file.name}")

        # Find orphaned PPTX files
        for pptx_file in self.course_directory.rglob("*.pptx"):
            if 'tutor_lms_output' in str(pptx_file):
                continue
            
            try:
                rel_path = str(pptx_file.relative_to(self.course_directory))
                # Normalize path separators
                rel_path = rel_path.replace('\\', '/')
            except ValueError:
                continue
            
            # Check against referenced files (which might use forward slashes)
            is_referenced = False
            for ref in referenced_files:
                if ref and ref.replace('\\', '/') == rel_path:
                    is_referenced = True
                    break
            
            if not is_referenced:
                page = self.pptx_parser.parse_pptx(pptx_file, identifier=f"orphaned_{pptx_file.stem}")
                if page:
                    pages.append(page)
                    print(f"  [OK] Converted orphaned PPTX: {pptx_file.name}")
        
        return pages
