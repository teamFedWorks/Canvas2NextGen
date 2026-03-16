import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import lxml.etree as ET

class CanvasExportParser:
    """
    Parses Canvas native export packages (containing course_export.json).
    """

    def __init__(self, extract_dir: Path):
        self.extract_dir = extract_dir
        self.export_json_path = extract_dir / "course_export.json"
        self.module_meta_path = extract_dir / "modules" / "module_meta.xml"

    def parse(self) -> Dict[str, Any]:
        """
        Parses course_export.json and module_meta.xml.
        """
        if not self.export_json_path.exists():
            return {"error": "course_export.json not found."}

        try:
            # 1. Parse main export JSON
            with open(self.export_json_path, 'r', encoding='utf-8') as f:
                export_data = json.load(f)

            title = export_data.get('course', {}).get('title', 'Untitled Course')
            description = export_data.get('course', {}).get('public_description', '')

            # 2. Parse Modules from module_meta.xml
            curriculum = self._parse_curriculum()

            # 3. Enrich with content from JSON/Files
            # (In a real implementation, we would map the module items to actual content)
            
            return {
                "title": title,
                "description": description,
                "curriculum": curriculum
            }
        except Exception as e:
            return {"error": f"Failed to parse Canvas export: {str(e)}"}

    def _parse_curriculum(self) -> List[Dict[str, Any]]:
        curriculum = []
        if not self.module_meta_path.exists():
            return curriculum

        try:
            tree = ET.parse(str(self.module_meta_path))
            root = tree.getroot()

            for module_elem in root.findall(".//module"):
                title = module_elem.findtext("title")
                items = []

                for item_elem in module_elem.findall(".//item"):
                    item_title = item_elem.findtext("title")
                    item_type = item_elem.findtext("content_type")
                    
                    # Map Canvas types to LessonItem types
                    mapped_type = self._map_type(item_type)
                    
                    items.append({
                        "title": item_title,
                        "type": mapped_type,
                        "content": "",  # To be filled during transformation/asset processing
                        "quizConfig": {},
                        "assignmentConfig": {},
                        "discussionConfig": {}
                    })

                curriculum.append({
                    "title": title,
                    "summary": "",
                    "items": items
                })

        except Exception:
            pass
            
        return curriculum

    def _map_type(self, canvas_type: str) -> str:
        mapping = {
            "WikiPage": "Lesson",
            "Quiz": "Quiz",
            "Assignment": "Assignment",
            "DiscussionTopic": "Discussion",
            "ExternalUrl": "ExternalLink",
            "Attachment": "Lesson"  # Usually a file download
        }
        return mapping.get(canvas_type, "Lesson")
