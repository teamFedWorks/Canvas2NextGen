"""
Discussion Parser - Parses Canvas discussion topics.

Extracts discussion prompts from discussion XML files (imsdt_xmlv1p1).
"""

from pathlib import Path
from typing import List, Optional
import html as html_module

from models.canvas_models import CanvasDiscussion, WorkflowState
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, get_element_text, get_inner_html
from utils.html_utils import sanitize_html

# Canvas discussion XML namespace
_DT_NAMESPACES = {
    'dt': 'http://www.imsglobal.org/xsd/imsccv1p1/imsdt_v1p1',
}


class DiscussionParser:
    """
    Parses Canvas discussion XML files.
    """
    
    def __init__(self, course_directory: Path):
        self.course_directory = course_directory
        self.errors: List[MigrationError] = []
    
    def parse_discussion(self, xml_file: Path) -> Optional[CanvasDiscussion]:
        """
        Parse a single discussion XML file.

        Canvas exports discussions as imsdt_xmlv1p1 XML files. The structure is:
          <topic xmlns="http://www.imsglobal.org/xsd/imsccv1p1/imsdt_v1p1">
            <title>Discussion Title</title>
            <text texttype="text/html">&lt;p&gt;HTML-escaped body&lt;/p&gt;</text>
          </topic>

        The default namespace means bare XPath './/title' won't match — we must
        use the namespaced XPath or fall back to iterating all elements.
        """
        try:
            root = parse_xml_file(xml_file)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.ERROR,
                    error_type="DISCUSSION_PARSE_ERROR",
                    message=f"Failed to parse discussion file: {xml_file.name}",
                    file_path=str(xml_file)
                ))
                return None

            # --- Title: try namespaced, then bare, then iterate ---
            title = (
                get_element_text(find_element(root, './/dt:title', _DT_NAMESPACES), "")
                or get_element_text(find_element(root, './/title', {}), "")
            )
            if not title:
                # Last resort: iterate all elements
                for el in root.iter():
                    if el.tag.split('}')[-1] == 'title' and el.text and el.text.strip():
                        title = el.text.strip()
                        break
            if not title:
                title = xml_file.stem

            # --- Body: try namespaced, then bare, then iterate ---
            body_elem = (
                find_element(root, './/dt:text', _DT_NAMESPACES)
                or find_element(root, './/text', {})
            )
            if body_elem is None:
                for el in root.iter():
                    if el.tag.split('}')[-1] == 'text':
                        body_elem = el
                        break

            body = ""
            if body_elem is not None:
                raw = get_element_text(body_elem, "")
                if raw and raw.strip():
                    # Canvas stores HTML as escaped entities — unescape to recover HTML
                    body = sanitize_html(html_module.unescape(raw))
                else:
                    # Fallback: try inner HTML (handles non-escaped CDATA)
                    inner = get_inner_html(body_elem)
                    if inner and inner.strip():
                        body = sanitize_html(html_module.unescape(inner))
            if not body:
                body = "<p>Discussion prompt — see course instructions.</p>"

            return CanvasDiscussion(
                title=title,
                identifier=xml_file.stem,
                body=body,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(xml_file)
            )

        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.ERROR,
                error_type="DISCUSSION_PARSE_ERROR",
                message=f"Unexpected error parsing discussion: {str(e)}",
                file_path=str(xml_file)
            ))
            return None

    def find_all_discussions(self) -> List[CanvasDiscussion]:
        discussions = []
        discussion_dirs = ["discussion_topics", "course_settings"]
        for d in discussion_dirs:
            target = self.course_directory / d
            if target.exists():
                for xml_file in target.glob("*.xml"):
                    disc = self.parse_discussion(xml_file)
                    if disc:
                        discussions.append(disc)
        return discussions
