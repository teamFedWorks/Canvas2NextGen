"""
WebLink Parser - Parses Canvas external web links.

Extracts external URLs from web link XML files (imswl_xmlv1p1).
"""

from pathlib import Path
from typing import List, Optional

from models.canvas_models import CanvasWebLink
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, get_element_text, get_element_attribute


# Canvas weblink XML uses this namespace on the root element
_WEBLINK_NAMESPACES = {
    'wl': 'http://www.imsglobal.org/xsd/imsccv1p1/imswl_v1p1',
}


class WebLinkParser:
    """
    Parses Canvas web link XML files.
    """
    
    def __init__(self, course_directory: Path):
        self.course_directory = course_directory
        self.errors: List[MigrationError] = []
    
    def parse_weblink(self, xml_file: Path) -> Optional[CanvasWebLink]:
        """
        Parse a single web link XML file.

        Canvas exports weblinks as imswl_xmlv1p1 XML files. The structure is:
          <webLink xmlns="http://www.imsglobal.org/xsd/imswl_v1p1">
            <title>Link Title</title>
            <url href="https://example.com"/>
          </webLink>

        The namespace is often present on the root element, so we try both
        namespaced and bare XPath lookups.
        """
        try:
            root = parse_xml_file(xml_file)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.ERROR,
                    error_type="WEBLINK_PARSE_ERROR",
                    message=f"Failed to parse web link file: {xml_file.name}",
                    file_path=str(xml_file)
                ))
                return None

            # --- Title: try namespaced then bare ---
            title = (
                get_element_text(find_element(root, './/wl:title', _WEBLINK_NAMESPACES), "")
                or get_element_text(find_element(root, './/title', {}), xml_file.stem)
            )

            # --- URL: try namespaced then bare ---
            url = ""
            for xpath, ns in [
                ('.//wl:url', _WEBLINK_NAMESPACES),
                ('.//url', {}),
            ]:
                url_elem = find_element(root, xpath, ns)
                if url_elem is not None:
                    url = get_element_attribute(url_elem, 'href')
                    if not url:
                        url = get_element_text(url_elem, "")
                    if url:
                        break

            if not url:
                # Last resort: scan all elements for an href attribute
                for elem in root.iter():
                    href = elem.get('href', '')
                    if href.startswith('http'):
                        url = href
                        break

            if not url:
                return None

            return CanvasWebLink(
                title=title or xml_file.stem,
                identifier=xml_file.stem,
                url=url.strip(),
                source_file=str(xml_file)
            )

        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.ERROR,
                error_type="WEBLINK_PARSE_ERROR",
                message=f"Unexpected error parsing web link: {str(e)}",
                file_path=str(xml_file)
            ))
            return None
