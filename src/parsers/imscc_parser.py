import os
import lxml.etree as ET
from pathlib import Path
from typing import List, Dict, Any, Optional

class IMSCCParser:
    """
    Parses IMS Common Cartridge (.imscc) packages.
    """
    
    NAMESPACES = {
        'ims': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1',
        'imscc': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1',
        'lom': 'http://ltsc.ieee.org/xsd/imsccv1p1/LOM/resource',
    }

    def __init__(self, extract_dir: Path):
        self.extract_dir = extract_dir
        self.manifest_path = extract_dir / "imsmanifest.xml"
        self.resources = {}
        self.errors = []

    def parse(self) -> Dict[str, Any]:
        """
        Parses the manifest and returns a structured course intermediate object.
        """
        if not self.manifest_path.exists():
            return {"error": "Manifest file not found."}

        try:
            tree = ET.parse(str(self.manifest_path))
            root = tree.getroot()

            # 1. Parse Metadata
            title = self._get_title(root)
            description = self._get_description(root)

            # 2. Map Resources
            self.resources = self._parse_resources(root)

            # 3. Parse Hierarchy (Organizations)
            curriculum = self._parse_curriculum(root)

            return {
                "title": title,
                "description": description,
                "curriculum": curriculum
            }
        except Exception as e:
            return {"error": f"Failed to parse IMSCC manifest: {str(e)}"}

    def _get_title(self, root) -> str:
        title_elem = root.find(".//ims:metadata/ims:title", self.NAMESPACES)
        if title_elem is None:
            # Fallback for some Canvas exports
            title_elem = root.find(".//imscc:title", self.NAMESPACES)
        return title_elem.text if title_elem is not None else "Untitled Course"

    def _get_description(self, root) -> str:
        desc_elem = root.find(".//ims:metadata/ims:description", self.NAMESPACES)
        return desc_elem.text if desc_elem is not None else ""

    def _parse_resources(self, root) -> Dict[str, Dict[str, str]]:
        resources = {}
        for res in root.findall(".//ims:resource", self.NAMESPACES):
            ident = res.get('identifier')
            res_type = res.get('type')
            href = res.get('href')
            
            # Find associated files
            files = [f.get('href') for f in res.findall("ims:file", self.NAMESPACES)]
            
            resources[ident] = {
                "type": res_type,
                "href": href,
                "files": files
            }
        return resources

    def _parse_curriculum(self, root) -> List[Dict[str, Any]]:
        curriculum = []
        # Find the organization
        org = root.find(".//ims:organization", self.NAMESPACES)
        if org is None:
            return curriculum

        # Modules are top-level items in the organization
        for module_item in org.findall("ims:item", self.NAMESPACES):
            module_title = module_item.find("ims:title", self.NAMESPACES).text
            module_id = module_item.get('identifier')
            
            items = []
            # Items within the module
            for child in module_item.findall("ims:item", self.NAMESPACES):
                item_data = self._parse_item(child)
                if item_data:
                    items.append(item_data)

            curriculum.append({
                "title": module_title,
                "summary": "",  # Canvas doesn't usually have module summaries
                "items": items
            })
            
        return curriculum

    def _parse_item(self, item_elem) -> Optional[Dict[str, Any]]:
        title_elem = item_elem.find("ims:title", self.NAMESPACES)
        title = title_elem.text if title_elem is not None else "Untitled Item"
        ident_ref = item_elem.get('identifierref')
        
        if not ident_ref or ident_ref not in self.resources:
            # Might be a sub-header or empty item
            return None

        resource = self.resources[ident_ref]
        res_type = resource['type']
        
        # Determine internal type
        internal_type = "Lesson"
        if "assessment" in res_type:
            internal_type = "Quiz"
        elif "assignment" in res_type:
            internal_type = "Assignment"
        elif "discussion" in res_type:
            internal_type = "Discussion"
        elif "webcontent" in res_type:
            internal_type = "Lesson"
        
        # Load content if it's a page/lesson
        content = ""
        if internal_type == "Lesson" and resource['href']:
            content = self._load_file_content(resource['href'])

        return {
            "title": title,
            "type": internal_type,
            "canvas_resource_id": ident_ref,
            "href": resource['href'],
            "content": content,
            "quizConfig": {},
            "assignmentConfig": {},
            "discussionConfig": {}
        }

    def _load_file_content(self, href: str) -> str:
        file_path = self.extract_dir / href
        if file_path.exists() and file_path.suffix in ['.html', '.htm', '.xml']:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return ""
        return ""
