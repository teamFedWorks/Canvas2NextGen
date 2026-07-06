"""
Manifest Parser - Single source of truth for course structure.

Parses imsmanifest.xml to extract course structure, modules, and resource references.

Fallback: when <organizations> is empty ("headless export"), the parser reconstructs
the curriculum directly from <resources> by grouping:
  - wiki_content/*.html  → weekly modules (week-N) or Course Info
  - discussions          → Discussions module
  - assignments          → Assignments module
  - LTI / weblinks       → External Resources module
"""

import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from models.canvas_models import (
    CanvasCourse,
    CanvasModule,
    CanvasModuleItem,
    CanvasResource,
    WorkflowState
)
from models.migration_report import MigrationError, ErrorSeverity
from config.canvas_schemas import IMS_CC_NAMESPACES, CANVAS_PATHS
from utils.xml_utils import (
    parse_xml_file,
    find_element,
    find_elements,
    get_element_text,
    get_element_attribute
)
from observability.logger import get_logger

logger = get_logger(__name__)


class ManifestParser:
    """
    Parses imsmanifest.xml to build course structure.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize manifest parser.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.manifest_path = course_directory / CANVAS_PATHS['MANIFEST']
        self.errors: List[MigrationError] = []
    
    def parse(self) -> Optional[CanvasCourse]:
        """
        Parse the manifest and build CanvasCourse structure.
        
        Returns:
            CanvasCourse object or None if parsing fails
        """
        try:
            root = parse_xml_file(self.manifest_path)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.CRITICAL,
                    error_type="MANIFEST_PARSE_ERROR",
                    message="Failed to parse imsmanifest.xml",
                    file_path=str(self.manifest_path)
                ))
                return None
            
            # Extract course metadata
            course_title = self._extract_course_title(root)
            course_id = get_element_attribute(root, 'identifier', 'unknown')
            
            # Build resource map
            resources = self._build_resource_map(root)
            
            # Parse organization (module structure)
            modules = self._parse_organization(root, resources)
            
            # Create course object
            course = CanvasCourse(
                title=course_title,
                identifier=course_id,
                modules=modules,
                resources=resources,
                source_directory=str(self.course_directory),
                created_at=datetime.now()
            )
            
            return course
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.CRITICAL,
                error_type="MANIFEST_PARSE_ERROR",
                message=f"Unexpected error parsing manifest: {str(e)}",
                file_path=str(self.manifest_path)
            ))
            return None
    
    def _extract_course_title(self, root) -> str:
        """
        Extract course title from manifest.
        
        Args:
            root: Manifest root element
            
        Returns:
            Course title
        """
        # Try with LOM namespace (imsmd) - This is standard for Canvas
        title_elem = find_element(root, './/imsmd:title/imsmd:string', IMS_CC_NAMESPACES)
        if title_elem is not None:
            return get_element_text(title_elem, "Untitled Course")

        # Try with CC namespace
        title_elem = find_element(root, './/imscc:title/imscc:string', IMS_CC_NAMESPACES)
        if title_elem is not None:
            return get_element_text(title_elem, "Untitled Course")
        
        # Try without namespace
        title_elem = find_element(root, './/title/string', {})
        if title_elem is not None:
            return get_element_text(title_elem, "Untitled Course")
        
        # Fallback: try simple title
        title_elem = find_element(root, './/title', {})
        if title_elem is not None:
            return get_element_text(title_elem, "Untitled Course")
        
        return "Untitled Course"
    
    def _build_resource_map(self, root) -> Dict[str, CanvasResource]:
        """
        Build a map of resource identifiers to resource objects.
        
        Args:
            root: Manifest root element
            
        Returns:
            Dictionary mapping resource IDs to CanvasResource objects
        """
        resource_map = {}
        
        # Find all resource elements
        resources = find_elements(root, './/imscc:resource', IMS_CC_NAMESPACES)
        
        # If not found with namespace, try without
        if not resources:
            resources = find_elements(root, './/resource', {})
        
        for resource_elem in resources:
            identifier = get_element_attribute(resource_elem, 'identifier')
            href = get_element_attribute(resource_elem, 'href')
            res_type = get_element_attribute(resource_elem, 'type')
            
            if identifier:
                # Normalise the href: replace Unicode non-breaking / narrow
                # no-break spaces (U+00A0, U+202F) with regular spaces so
                # Path resolution works on all platforms.  Canvas sometimes
                # encodes filenames with these characters in the manifest.
                if href:
                    href = (
                        href
                        .replace('\u00a0', ' ')   # NO-BREAK SPACE
                        .replace('\u202f', ' ')   # NARROW NO-BREAK SPACE
                    )

                # Check if file exists
                file_exists = False
                resolved_path = None
                
                if href:
                    file_path = self.course_directory / href
                    file_exists = file_path.exists()
                    if not file_exists:
                        # Windows cannot create paths with colons — zip extractors
                        # replace ':' with '_'.  Try the sanitised variant.
                        sanitised = href.replace(':', '_')
                        if sanitised != href:
                            alt_path = self.course_directory / sanitised
                            if alt_path.exists():
                                href = sanitised
                                file_path = alt_path
                                file_exists = True
                    if file_exists:
                        resolved_path = str(file_path)
                
                # Extract nested <file> tags for embedded assets
                nested_files = []
                file_elems = resource_elem.findall('./file')
                if not file_elems:
                    file_elems = find_elements(resource_elem, './imscc:file', IMS_CC_NAMESPACES)
                for f_elem in file_elems:
                    f_href = get_element_attribute(f_elem, 'href')
                    if f_href:
                        f_href = f_href.replace('\u00a0', ' ').replace('\u202f', ' ')
                        f_path = self.course_directory / f_href
                        if not f_path.exists():
                            f_sanitised = f_href.replace(':', '_')
                            if (self.course_directory / f_sanitised).exists():
                                f_href = f_sanitised
                        nested_files.append(f_href)
                
                resource = CanvasResource(
                    identifier=identifier,
                    href=href,
                    type=res_type,
                    files=nested_files,
                    file_exists=file_exists,
                    resolved_path=resolved_path
                )
                
                resource_map[identifier] = resource
        
        return resource_map
    
    def _parse_organization(
        self,
        root,
        resources: Dict[str, CanvasResource]
    ) -> List[CanvasModule]:
        """
        Parse the organization section to extract module structure.
        
        Args:
            root: Manifest root element
            resources: Resource map
            
        Returns:
            List of CanvasModule objects
        """
        modules = []

        # Find organization element
        org = find_element(root, './/imscc:organization', IMS_CC_NAMESPACES)
        if org is None:
            org = find_element(root, './/organization', {})

        if org is None:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="NO_ORGANIZATION",
                message="No organization element found in manifest",
                suggested_action="Course may have no module structure"
            ))
            return modules

        # Find all top-level items (modules)
        items = org.findall('./item')
        if not items:
            items = find_elements(org, './imscc:item', IMS_CC_NAMESPACES)

        # Canvas export often has a single root item (LearningModules) wrapping everything
        if len(items) == 1:
            root_item = items[0]
            children = root_item.findall('./item')
            if not children:
                children = find_elements(root_item, './imscc:item', IMS_CC_NAMESPACES)
            if children:
                logger.debug("Detected wrapper module, flattening structure...")
                items = children
            else:
                # ── Headless export fallback ──────────────────────────────────
                # The LearningModules wrapper is EMPTY — the instructor exported
                # the course without publishing modules. All content still lives
                # in <resources> (wiki pages, discussions, assignments, LTI).
                # Reconstruct the curriculum from the resource map.
                logger.warning(
                    "Headless Canvas export detected: <organizations> is empty. "
                    "Reconstructing curriculum from <resources>."
                )
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.WARNING,
                    error_type="HEADLESS_EXPORT",
                    message=(
                        "Course has no module structure in manifest (LearningModules is empty). "
                        "Curriculum reconstructed from resources."
                    ),
                    suggested_action="Ask instructor to re-export with modules published, or verify reconstructed structure."
                ))
                return self._build_curriculum_from_resources(resources)

        for position, item_elem in enumerate(items):
            module = self._parse_module_item(item_elem, resources, position)
            if module:
                modules.append(module)

        return modules

    # ── Headless-export fallback ───────────────────────────────────────────────

    def _build_curriculum_from_resources(
        self,
        resources: Dict[str, "CanvasResource"]
    ) -> List["CanvasModule"]:
        """
        Reconstruct course curriculum when <organizations> is empty.

        Grouping strategy:
          - wiki_content/week-N.html  → individual week modules (Week 1 … Week N)
          - other wiki pages          → 'Course Information' module
          - webcontent (non-wiki)     → included in appropriate module
          - discussions               → 'Discussions' module
          - assignments               → 'Assignments' module
          - LTI / weblinks            → 'External Resources' module
          - syllabus / course-settings → skipped (internal Canvas metadata)
        """
        from models.canvas_models import CanvasModule, CanvasModuleItem, WorkflowState

        # Buckets
        week_pages: Dict[int, List] = {}   # week_number → [resource items]
        info_pages:  List = []             # non-week wiki pages
        discussions: List = []
        assignments: List = []
        lti_links:   List = []

        _SKIP_TYPES = {
            "associatedcontent/imscc_xmlv1p1/learning-application-resource",
        }
        _SKIP_HREFS = ("course_settings/",)

        for res_id, resource in resources.items():
            res_type = (resource.type or "").lower()
            href     = resource.href or ""

            # Skip internal Canvas metadata
            if resource.type in _SKIP_TYPES:
                continue
            if any(href.startswith(s) for s in _SKIP_HREFS):
                continue

            # ── Discussions ────────────────────────────────────────────────────
            if "discussion" in res_type or "imsdt" in res_type:
                name = self._label_from_href(href) or res_id
                discussions.append((res_id, name, resource))

            # ── Assignments ────────────────────────────────────────────────────
            elif "assignment" in res_type:
                name = self._label_from_href(href) or res_id
                assignments.append((res_id, name, resource))

            # ── LTI / external tools ───────────────────────────────────────────
            elif "lti" in res_type or "weblink" in res_type or "imswl" in res_type:
                name = self._label_from_lti_xml(res_id, resource.href) or self._label_from_href(href) or "External Resource"
                lti_links.append((res_id, name, resource))

            # ── Wiki / webcontent pages ────────────────────────────────────────
            elif "webcontent" in res_type or href.startswith("wiki_content/"):
                # Detect week pages: wiki_content/week-3.html → week 3
                week_match = re.search(r"week[-_](\d+)", href, re.IGNORECASE)
                if week_match:
                    wk = int(week_match.group(1))
                    week_pages.setdefault(wk, []).append((res_id, f"Week {wk}", resource))
                else:
                    name = self._label_from_href(href) or res_id
                    info_pages.append((res_id, name, resource))

        # ── Assemble modules ───────────────────────────────────────────────────
        modules: List[CanvasModule] = []
        pos = 0

        # Course Information (non-week wiki pages)
        if info_pages:
            modules.append(self._make_synthetic_module(
                "Course Information", info_pages, pos, content_type_override="page"
            ))
            pos += 1

        # Weekly modules in order
        for wk in sorted(week_pages.keys()):
            modules.append(self._make_synthetic_module(
                f"Week {wk}", week_pages[wk], pos, content_type_override="page"
            ))
            pos += 1

        # Assignments module
        if assignments:
            modules.append(self._make_synthetic_module(
                "Assignments", assignments, pos, content_type_override="assignment"
            ))
            pos += 1

        # Discussions module
        if discussions:
            modules.append(self._make_synthetic_module(
                "Discussions", discussions, pos, content_type_override="discussion"
            ))
            pos += 1

        # External Resources module (LTI / weblinks)
        if lti_links:
            modules.append(self._make_synthetic_module(
                "External Resources", lti_links, pos, content_type_override="external_tool"
            ))
            pos += 1

        logger.info(
            f"Headless-export reconstruction complete: {len(modules)} synthetic modules, "
            f"{sum(len(m.items) for m in modules)} total items."
        )
        return modules

    def _label_from_lti_xml(self, res_id: str, href: Optional[str]) -> str:
        """
        Read the <blti:title> from an LTI XML file to get a human-readable name.
        Canvas LTI resources are stored in lti_resource_links/<res_id>.xml or at href.
        """
        # Try the standard LTI resource_links location first
        candidates = [
            self.course_directory / "lti_resource_links" / f"{res_id}.xml",
        ]
        if href:
            candidates.append(self.course_directory / href)

        lti_ns = "http://www.imsglobal.org/xsd/imsbasiclti_v1p0"
        for candidate in candidates:
            if candidate.exists():
                try:
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(str(candidate))
                    root = tree.getroot()
                    # Try namespaced title first
                    title_elem = root.find(f"{{{lti_ns}}}title")
                    if title_elem is None:
                        # Try without namespace
                        title_elem = root.find(".//title")
                    if title_elem is not None and title_elem.text:
                        return title_elem.text.strip()
                except Exception:
                    pass
        return ""

    def _label_from_href(self, href: str) -> str:
        """Derive a human-readable label from a resource href path."""
        if not href:
            return ""
        stem = Path(href).stem  # e.g. 'week-1', 'chapter-2-cognifying'
        # Replace hyphens/underscores with spaces, title-case
        label = re.sub(r"[-_]+", " ", stem).strip().title()
        return label

    def _make_synthetic_module(
        self,
        title: str,
        items: List,
        position: int,
        content_type_override: Optional[str] = None,
    ) -> "CanvasModule":
        """Build a CanvasModule from a list of (res_id, label, resource) tuples."""
        from models.canvas_models import CanvasModule, CanvasModuleItem, WorkflowState
        import uuid

        module_items = []
        for i, (res_id, label, resource) in enumerate(items):
            c_type = content_type_override or (
                "page" if "webcontent" in (resource.type or "").lower() else "page"
            )
            item = CanvasModuleItem(
                title=label,
                identifier=f"synthetic_{res_id}",
                content_type=c_type,
                content_file=resource.href,
                items=[],
                position=i,
                workflow_state=WorkflowState.ACTIVE,
            )
            item._content_ref = res_id
            module_items.append(item)

        return CanvasModule(
            title=title,
            identifier=f"synthetic_module_{position}",
            position=position,
            items=module_items,
            workflow_state=WorkflowState.ACTIVE,
        )
    
    def _parse_module_item(
        self,
        item_elem,
        resources: Dict[str, CanvasResource],
        position: int = 0
    ) -> Optional[CanvasModule]:
        """
        Parse a module item element.
        
        Args:
            item_elem: Item XML element
            resources: Resource map
            position: Module position
            
        Returns:
            CanvasModule object or None
        """
        # Get module title
        title_elem = item_elem.find('./title')
        if title_elem is None:
            title_elem = find_element(item_elem, './imscc:title', IMS_CC_NAMESPACES)
        
        title = get_element_text(title_elem, "Untitled Module")
        identifier = get_element_attribute(item_elem, 'identifier')
        
        # Parse child items
        child_items = []
        children = item_elem.findall('./item')
        if not children:
            children = find_elements(item_elem, './imscc:item', IMS_CC_NAMESPACES)
        
        for child_position, child_elem in enumerate(children):
            child_item = self._parse_child_item(child_elem, resources, child_position)
            if child_item:
                child_items.append(child_item)
        
        module = CanvasModule(
            title=title,
            identifier=identifier,
            position=position,
            items=child_items,
            workflow_state=WorkflowState.ACTIVE
        )
        
        return module
    
    def _parse_child_item(
        self,
        item_elem,
        resources: Dict[str, CanvasResource],
        position: int = 0
    ) -> Optional[CanvasModuleItem]:
        """
        Parse a child item within a module.
        
        Args:
            item_elem: Item XML element
            resources: Resource map
            position: Item position
            
        Returns:
            CanvasModuleItem object or None
        """
        title_elem = item_elem.find('./title')
        if title_elem is None:
            title_elem = find_element(item_elem, './imscc:title', IMS_CC_NAMESPACES)
        
        title = get_element_text(title_elem, "Untitled Item")
        identifier = get_element_attribute(item_elem, 'identifier')
        identifierref = get_element_attribute(item_elem, 'identifierref')
        
        # Determine content type and file from resource.
        # IMPORTANT: we store the identifierref (resource ID) as the content_ref so
        # the transformer can look up the correct page/quiz/assignment by resource ID.
        content_type = None
        content_file = None
        content_ref = identifierref  # the resource identifier — used for content lookup
        
        if identifierref and identifierref in resources:
            resource = resources[identifierref]
            content_file = resource.href
            
            # Infer content type from resource type
            if resource.type:
                res_type = resource.type.lower()
                if 'assessment' in res_type:
                    content_type = 'quiz'
                elif 'assignment' in res_type:
                    content_type = 'assignment'
                elif 'webcontent' in res_type:
                    content_type = 'page'
                elif 'discussion' in res_type or 'imsdt' in res_type:
                    content_type = 'discussion'
                elif 'weblink' in res_type or 'imswl' in res_type:
                    content_type = 'weblink'
                elif 'associatedcontent' in res_type:
                    # AssociatedContent wraps assignments — the href points to the
                    # assignment subfolder XML, so treat as assignment.
                    content_type = 'assignment'
        
        # Parse nested items (sub-items)
        nested_items = []
        children = item_elem.findall('./item')
        if not children:
            children = find_elements(item_elem, './imscc:item', IMS_CC_NAMESPACES)
        
        for child_position, child_elem in enumerate(children):
            nested_item = self._parse_child_item(child_elem, resources, child_position)
            if nested_item:
                nested_items.append(nested_item)
        
        module_item = CanvasModuleItem(
            title=title,
            identifier=identifier,
            content_type=content_type,
            content_file=content_file,
            # Store the resource identifierref so the transformer can resolve content
            items=nested_items,
            position=position,
            workflow_state=WorkflowState.ACTIVE
        )
        # Attach the resource ref as a custom attribute for transformer lookup
        module_item._content_ref = content_ref
        
        return module_item
