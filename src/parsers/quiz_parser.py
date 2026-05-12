"""
Quiz Parser - Parses Canvas quizzes/assessments.

Extracts quiz data from assessment XML files.
"""

from pathlib import Path
from typing import List, Optional
from datetime import datetime

from models.canvas_models import CanvasQuiz, WorkflowState
from models.migration_report import MigrationError, ErrorSeverity
from utils.xml_utils import parse_xml_file, find_element, find_elements, get_element_text, get_element_attribute
from utils.html_utils import sanitize_html
from .question_parser import QuestionParser


class QuizParser:
    """
    Parses Canvas quiz/assessment XML files.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize quiz parser.
        
        Args:
            course_directory: Path to Canvas course export directory
        """
        self.course_directory = course_directory
        self.question_parser = QuestionParser(course_directory)
        self.errors: List[MigrationError] = []
    
    def parse_quiz(self, quiz_dir: Path) -> Optional[CanvasQuiz]:
        """
        Parse a quiz from its directory.
        
        Args:
            quiz_dir: Path to quiz directory
            
        Returns:
            CanvasQuiz object or None if parsing fails
        """
        # Look for assessment XML files
        assessment_file = quiz_dir / "assessment_meta.xml"
        if not assessment_file.exists():
            assessment_file = quiz_dir / "assessment.xml"
        
        if not assessment_file.exists():
            return None
        
        try:
            root = parse_xml_file(assessment_file)
            if root is None:
                self.errors.append(MigrationError(
                    severity=ErrorSeverity.ERROR,
                    error_type="QUIZ_PARSE_ERROR",
                    message=f"Failed to parse quiz: {quiz_dir.name}",
                    file_path=str(assessment_file)
                ))
                return None
            
            # Extract quiz metadata
            title = self._extract_title(root, quiz_dir)
            description = self._extract_description(root)
            quiz_type = get_element_text(find_element(root, './/quiz_type', {}), "assignment")
            points_possible = float(get_element_text(find_element(root, './/points_possible', {}), "0"))
            time_limit = self._extract_time_limit(root)
            allowed_attempts = int(get_element_text(find_element(root, './/allowed_attempts', {}), "1"))

            # Detect Respondus LockDown Browser requirement.
            # This is stored as <require_lockdown_browser>true</require_lockdown_browser>
            # in assessment_meta.xml.  The quiz questions ARE fully exported in the QTI
            # file — only the browser enforcement setting needs manual configuration.
            def _bool_field(tag: str) -> bool:
                elem = find_element(root, f'.//{tag}', {})
                if elem is None:
                    # Wildcard namespace search
                    for el in root.iter():
                        if el.tag.split('}')[-1] == tag:
                            return (el.text or '').strip().lower() == 'true'
                    return False
                return (get_element_text(elem, 'false')).strip().lower() == 'true'

            require_lockdown_browser = _bool_field('require_lockdown_browser')
            require_lockdown_browser_for_results = _bool_field('require_lockdown_browser_for_results')

            # Parse questions
            questions = self.question_parser.parse_questions_from_quiz(quiz_dir)
            
            quiz = CanvasQuiz(
                title=title,
                identifier=quiz_dir.name,
                description=description,
                quiz_type=quiz_type,
                points_possible=points_possible,
                time_limit=time_limit,
                allowed_attempts=allowed_attempts,
                questions=questions,
                workflow_state=WorkflowState.ACTIVE,
                source_file=str(assessment_file),
                require_lockdown_browser=require_lockdown_browser,
                require_lockdown_browser_for_results=require_lockdown_browser_for_results,
            )
            
            return quiz
            
        except Exception as e:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.ERROR,
                error_type="QUIZ_PARSE_ERROR",
                message=f"Unexpected error parsing quiz: {str(e)}",
                file_path=str(assessment_file)
            ))
            return None
    
    def _extract_title(self, root, quiz_dir: Path) -> str:
        """Extract quiz title — handles both namespaced and plain XML."""
        # Try plain (no namespace)
        title_elem = find_element(root, './/title', {})
        if title_elem is not None and title_elem.text:
            return title_elem.text.strip()
        # Try Canvas namespace
        from config.canvas_schemas import CANVAS_NAMESPACES
        title_elem = find_element(root, './/canvas:title', CANVAS_NAMESPACES)
        if title_elem is not None and title_elem.text:
            return title_elem.text.strip()
        # Wildcard namespace fallback
        for el in root.iter():
            if el.tag.split('}')[-1] == 'title' and el.text and el.text.strip():
                return el.text.strip()
        return quiz_dir.name

    def _extract_description(self, root) -> str:
        """Extract quiz description — handles namespaced XML."""
        import html
        for tag_name in ('description', 'rubric'):
            desc_elem = find_element(root, f'.//{tag_name}', {})
            if desc_elem is not None:
                text = get_element_text(desc_elem, "")
                return sanitize_html(html.unescape(text)) if text else ""
            # Wildcard namespace
            for el in root.iter():
                if el.tag.split('}')[-1] == tag_name and el.text:
                    return sanitize_html(html.unescape(el.text))
        return ""
    
    def _extract_time_limit(self, root) -> Optional[int]:
        """Extract time limit in minutes"""
        time_elem = find_element(root, './/time_limit', {})
        if time_elem is not None:
            try:
                return int(get_element_text(time_elem, "0"))
            except ValueError:
                pass
        return None
    
    def find_all_quizzes(self) -> List[CanvasQuiz]:
        """
        Find and parse all quizzes in course directory.
        Scans:
          - non_cc_assessments/ subdirectories
          - Any root-level subdirectory containing assessment_meta.xml or assessment_qti.xml
            (Canvas exports QTI into resource-ID-named folders at the course root)
        """
        quizzes = []
        seen_dirs = set()

        def _try_parse(quiz_dir: Path):
            if quiz_dir in seen_dirs:
                return
            seen_dirs.add(quiz_dir)
            quiz = self.parse_quiz(quiz_dir)
            if quiz:
                quizzes.append(quiz)

        # non_cc_assessments/ subdirectories
        assessments_dir = self.course_directory / "non_cc_assessments"
        if assessments_dir.exists():
            for quiz_dir in assessments_dir.iterdir():
                if quiz_dir.is_dir():
                    _try_parse(quiz_dir)

        # Root-level subdirectories that contain QTI files
        for item in self.course_directory.iterdir():
            if not item.is_dir() or item.name in ("non_cc_assessments", "wiki_content",
                                                    "web_resources", "course_settings",
                                                    "lti_resource_links"):
                continue
            if (item / "assessment_meta.xml").exists() or (item / "assessment_qti.xml").exists():
                _try_parse(item)

        return quizzes
