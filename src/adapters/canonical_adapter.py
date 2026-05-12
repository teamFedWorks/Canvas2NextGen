"""
Canonical Adapter Interface and Implementation.

All LMS adapters MUST implement this interface to produce CanonicalCourse models.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from models.canonical_models import CanonicalCourse, CanonicalModule, CanonicalCurriculumItem, CanonicalContentType, CanonicalAssessment, CanonicalQuestion, CanonicalQuestionType, SourcePlatform, CanonicalAsset
from core.classifier import ClassificationResult
from observability.logger import get_logger

logger = get_logger(__name__)


class BaseCanonicalAdapter(ABC):
    """
    Abstract base class for all LMS adapters.
    
    Each adapter MUST:
    1. Convert platform-specific format to CanonicalCourse
    2. Preserve all content including questions
    3. Track source identifiers for lineage
    4. Return parsing warnings for any issues
    """
    
    def __init__(self, source_path: Path):
        self.source_path = Path(source_path)
        self.warnings: list = []
    
    @abstractmethod
    def load(self, payload: dict) -> CanonicalCourse:
        """
        Parse the source and return a CanonicalCourse.
        
        Args:
            payload: Source-specific payload (e.g., {"zip_path": path})
            
        Returns:
            CanonicalCourse with all content properly mapped
        """
        pass
    
    def _add_warning(self, message: str):
        """Track parsing issues for reporting."""
        self.warnings.append(message)
        logger.warning(f"[{self.__class__.__name__}] {message}")


class CanvasToCanonicalAdapter(BaseCanonicalAdapter):
    """
    Converts CanvasCanvas models to Canonical models.
    
    This adapter wraps the existing Canvas parsing logic and normalizes output.
    """
    
    def __init__(self, source_path: Path):
        super().__init__(source_path)
        # Import here to avoid circular imports
        from adapters.canvas_adapter import CanvasAdapter
        self._canvas_adapter = CanvasAdapter()
    
    def load(self, payload: dict) -> CanonicalCourse:
        """Load Canvas export and convert to canonical format."""
        from models.canvas_models import CanvasCourse, CanvasModule as CanvasModuleModel
        
        # Use existing Canvas adapter to parse
        canvas_course = self._canvas_adapter.load(payload)
        
        # Convert to canonical
        return self._convert_to_canonical(canvas_course)
    
    def _convert_to_canonical(self, canvas_course) -> CanonicalCourse:
        """Convert CanvasCourse to CanonicalCourse."""
        
        # Build assessment lookup maps first (before modules)
        assessments = self._convert_assessments(canvas_course)
        assessment_map = {a.identifier: a for a in assessments}
        
        # Build question lookup for quick reference
        question_map = {}
        for assessment in assessments:
            for q in assessment.questions:
                question_map[q.identifier] = q
        
        # Build question bank lookup
        question_banks = self._convert_question_banks(canvas_course)
        for bank in question_banks:
            for q in bank.questions:
                question_map[q.identifier] = q
        
        # Convert modules with proper item references
        modules = []
        for canvas_module in canvas_course.modules:
            canonical_module = self._convert_module(
                canvas_module, 
                question_map,
                assessment_map,
                canvas_course.resources
            )
            modules.append(canonical_module)
        
        # Convert assets
        assets = self._convert_assets(canvas_course.resources, str(self.source_path))
        
        # Build canonical course
        canonical = CanonicalCourse(
            identifier=canvas_course.identifier or canvas_course.title,
            title=canvas_course.title,
            source_platform=SourcePlatform.CANVAS,
            source_course_id=canvas_course.identifier,
            modules=modules,
            assessments=assessments,
            assets=assets,
            description=canvas_course.title,
            source_directory=canvas_course.source_directory,
            parsing_warnings=self.warnings
        )
        
        return canonical
    
    def _convert_assessments(self, canvas_course) -> list:
        """Convert Canvas quizzes to canonical assessments."""
        assessments = []
        
        for quiz in canvas_course.quizzes:
            questions = []
            for cq in quiz.questions:
                q = CanonicalQuestion(
                    identifier=cq.identifier,
                    text=cq.question_text,
                    type=self._map_question_type(cq.question_type),
                    points=cq.points_possible or 1.0,
                    answers=[{
                        "id": ca.id,
                        "text": ca.text,
                        "isCorrect": float(ca.weight or 0) >= 100.0
                    } for ca in cq.answers],
                    position=cq.position,
                    source_file=cq.source_file
                )
                questions.append(q)
            
            assessment = CanonicalAssessment(
                identifier=f"quiz_{quiz.identifier}",
                title=quiz.title,
                description=quiz.description or "",
                is_graded=quiz.quiz_type == "assignment",
                assessment_type="quiz",
                questions=questions,
                points_possible=quiz.points_possible or 100.0,
                time_limit_minutes=quiz.time_limit,
                allowed_attempts=quiz.allowed_attempts or 1,
                shuffle_answers=quiz.shuffle_answers,
                show_correct_answers=quiz.show_correct_answers,
                require_lockdown_browser=quiz.require_lockdown_browser,
                source_file=quiz.source_file
            )
            assessments.append(assessment)
        
        return assessments
    
    def _convert_question_banks(self, canvas_course) -> list:
        """Convert Canvas question banks to questions."""
        # Note: Question banks are flattened into questions list
        questions = []
        for bank in canvas_course.question_banks:
            for q in bank.questions:
                questions.append(CanonicalQuestion(
                    identifier=f"bank_{q.identifier}",
                    text=q.question_text,
                    type=self._map_question_type(q.question_type),
                    points=q.points_possible or 1.0,
                    position=q.position,
                    source_file=q.source_file
                ))
        return questions
    
    def _convert_module(self, canvas_module, question_map, assessment_map, resources):
        """Convert a Canvas module."""
        items = []
        
        for item in canvas_module.items:
            content_type = self._map_content_type(item.content_type)
            
            # Build item
            canonical_item = CanonicalCurriculumItem(
                identifier=item.identifier or f"item_{id(item)}",
                title=item.title,
                content_type=content_type,
                position=item.position or 0,
                source_identifier=item.identifier
            )
            
            # Link to assessment if applicable
            if content_type == CanonicalContentType.QUIZ:
                assessment_key = f"quiz_{item.identifier}"
                if assessment_key in assessment_map:
                    canonical_item.assessment_ref = assessment_key
            
            items.append(canonical_item)
        
        return CanonicalModule(
            identifier=canvas_module.identifier or f"module_{id(canvas_module)}",
            title=canvas_module.title,
            items=items,
            position=canvas_module.position
        )
    
    def _convert_assets(self, resources, source_dir):
        """Convert manifest resources to canonical assets."""
        assets = []
        for res_id, res in resources.items():
            if res.href:
                asset = CanonicalAsset(
                    identifier=res_id,
                    filename=Path(res.href).name,
                    source_path=res.href,
                    mime_type="application/octet-stream"
                )
                assets.append(asset)
        return assets
    
    def _map_question_type(self, canvas_type) -> CanonicalQuestionType:
        """Map Canvas question type to canonical."""
        type_map = {
            "multiple_choice_question": CanonicalQuestionType.MULTIPLE_CHOICE,
            "true_false_question": CanonicalQuestionType.TRUE_FALSE,
            "fill_in_multiple_blanks_question": CanonicalQuestionType.FILL_BLANK,
            "essay_question": CanonicalQuestionType.ESSAY,
            "short_answer_question": CanonicalQuestionType.SHORT_ANSWER,
            "matching_question": CanonicalQuestionType.MATCHING,
            "numerical_question": CanonicalQuestionType.NUMERICAL,
            "file_upload_question": CanonicalQuestionType.FILE_UPLOAD,
            "ordering_question": CanonicalQuestionType.ORDERING,
            "categorization_question": CanonicalQuestionType.CATEGORIZATION,
        }
        if canvas_type:
            return type_map.get(canvas_type.value if hasattr(canvas_type, 'value') else str(canvas_type), CanonicalQuestionType.UNKNOWN)
        return CanonicalQuestionType.UNKNOWN
    
    def _map_content_type(self, canvas_type: str) -> CanonicalContentType:
        """Map Canvas content type to canonical."""
        if not canvas_type:
            return CanonicalContentType.LESSON
        
        type_map = {
            "page": CanonicalContentType.LESSON,
            "quiz": CanonicalContentType.QUIZ,
            "assignment": CanonicalContentType.ASSIGNMENT,
            "discussion": CanonicalContentType.DISCUSSION,
            "weblink": CanonicalContentType.WEBLINK,
        }
        return type_map.get(canvas_type.lower(), CanonicalContentType.LESSON)


class BlackboardToCanonicalAdapter(BaseCanonicalAdapter):
    """
    Converts Blackboard exports to Canonical models.
    """
    
    def __init__(self, source_path: Path):
        super().__init__(source_path)
        # Import here to avoid circular imports
        from adapters.blackboard_adapter import BlackboardAdapter
        self._bb_adapter = BlackboardAdapter()
    
    def load(self, payload: dict) -> CanonicalCourse:
        """Load Blackboard export and convert to canonical format."""
        # Use existing Blackboard adapter
        bb_course = self._bb_adapter.load(payload)
        
        # Convert to canonical (similar to Canvas)
        # This would reuse the Canvas adapter logic since both produce similar structures
        canvas_adapter = CanvasToCanonicalAdapter(self.source_path)
        return canvas_adapter._convert_to_canonical(bb_course)