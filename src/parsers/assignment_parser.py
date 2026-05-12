"""
Assignment Parser - Parses Canvas assignments.

Extracts assignment data from assignment_settings.xml files.
"""

from pathlib import Path
from typing import List, Optional
from datetime import datetime

from models.canvas_models import CanvasAssignment, SubmissionType, WorkflowState
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, find_elements, get_element_text
from utils.html_utils import sanitize_html
from config.canvas_schemas import CANVAS_NAMESPACES


class AssignmentParser:
    """
    Parses Canvas assignment XML files.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize assignment parser.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.errors: List[MigrationError] = []
    
    def parse_assignment(self, assignment_dir: Path) -> Optional[CanvasAssignment]:
        """
        Parse an assignment from its directory.
        
        Args:
            assignment_dir: Path to assignment directory
            
        Returns:
            CanvasAssignment object or None if parsing fails
        """
        settings_file = assignment_dir / "assignment_settings.xml"
        
        if not settings_file.exists():
            return None
        
        try:
            root = parse_xml_file(settings_file)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.ERROR,
                    error_type="ASSIGNMENT_PARSE_ERROR",
                    message=f"Failed to parse assignment settings: {assignment_dir.name}",
                    file_path=str(settings_file)
                ))
                return None
            
            # Extract assignment data
            title = get_element_text(find_element(root, './/canvas:title', CANVAS_NAMESPACES), "Untitled Assignment")
            description = self._extract_description(root)
            
            # Fallback: Check for HTML file if description is empty
            if not description:
                html_files = list(assignment_dir.glob("*.html"))
                if html_files:
                    try:
                        from utils.html_utils import get_body_content
                        import sys, os
                        target_html = html_files[0]
                        # On Windows, use extended-length path prefix to bypass MAX_PATH (260 chars)
                        path_str = str(target_html.resolve())
                        if sys.platform == 'win32' and len(path_str) > 255 and not path_str.startswith('\\\\?\\'):
                            path_str = '\\\\?\\' + path_str
                        with open(path_str, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                            description = get_body_content(html_content) or html_content
                    except Exception as e:
                        print(f"Warning: Failed to read HTML description from {html_files[0]}: {e}")
            
            points_possible = float(get_element_text(find_element(root, './/canvas:points_possible', CANVAS_NAMESPACES), "0"))
            grading_type = get_element_text(find_element(root, './/canvas:grading_type', CANVAS_NAMESPACES), "points")
            submission_types = self._extract_submission_types(root)
            workflow_state = self._extract_workflow_state(root)
            
            assignment = CanvasAssignment(
                title=title,
                identifier=assignment_dir.name,
                description=description,
                points_possible=points_possible,
                grading_type=grading_type,
                submission_types=submission_types,
                workflow_state=workflow_state,
                source_file=str(settings_file)
            )
            
            return assignment
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.ERROR,
                error_type="ASSIGNMENT_PARSE_ERROR",
                message=f"Unexpected error parsing assignment: {str(e)}",
                file_path=str(settings_file)
            ))
            return None
    
    def _extract_description(self, root) -> str:
        """Extract assignment description"""
        desc_elem = find_element(root, './/canvas:description', CANVAS_NAMESPACES)
        if desc_elem is not None:
            return sanitize_html(get_element_text(desc_elem, ""))
        return ""
    
    def _extract_submission_types(self, root) -> List[SubmissionType]:
        """Extract submission types"""
        submission_types = []
        types_elem = find_element(root, './/canvas:submission_types', CANVAS_NAMESPACES)
        
        if types_elem is not None:
            types_text = get_element_text(types_elem, "")
            for type_str in types_text.split(','):
                type_str = type_str.strip()
                try:
                    submission_types.append(SubmissionType(type_str))
                except ValueError:
                    pass  # Skip unknown types
        
        return submission_types
    
    def _extract_workflow_state(self, root) -> WorkflowState:
        """Extract workflow state"""
        state_elem = find_element(root, './/canvas:workflow_state', CANVAS_NAMESPACES)
        if state_elem is not None:
            state_text = get_element_text(state_elem, "active").lower()
            if state_text == "unpublished":
                return WorkflowState.UNPUBLISHED
            elif state_text == "deleted":
                return WorkflowState.DELETED
        return WorkflowState.ACTIVE
    
    def find_all_assignments(self) -> List[CanvasAssignment]:
        """
        Find and parse all assignments in course directory.
        
        Returns:
            List of CanvasAssignment objects
        """
        assignments = []
        
        # Find all directories with assignment_settings.xml
        for item in self.course_directory.iterdir():
            if item.is_dir():
                settings_file = item / "assignment_settings.xml"
                if settings_file.exists():
                    assignment = self.parse_assignment(item)
                    if assignment:
                        assignments.append(assignment)
        
        return assignments
