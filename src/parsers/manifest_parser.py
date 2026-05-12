"""
Manifest Parser - Single source of truth for course structure.

Parses imsmanifest.xml to extract course structure, modules, and resource references.
"""

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
        
        # If not found, try with namespace
        if not items:
            items = find_elements(org, './imscc:item', IMS_CC_NAMESPACES)
        
        # Canvas export often has a single root item wrapping everything
        # If we find exactly one item, we should check if it's a wrapper
        if len(items) == 1:
            root_item = items[0]
            # Check for children
            children = root_item.findall('./item')
            if not children:
                children = find_elements(root_item, './imscc:item', IMS_CC_NAMESPACES)
            
            if children:
                # Use the children as the top-level modules
                logger.debug("Detected wrapper module, flattening structure...")
                items = children
        
        for position, item_elem in enumerate(items):
            module = self._parse_module_item(item_elem, resources, position)
            if module:
                modules.append(module)
        
        return modules
    
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
