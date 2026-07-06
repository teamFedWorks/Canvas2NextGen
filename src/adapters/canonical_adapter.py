"""
Canonical Adapter Interface and Implementation.

All LMS adapters MUST implement this interface to produce CanonicalCourse models.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

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
    Converts Canvas exports to Canonical models.
    
    This adapter wraps the existing ZipAdapter/Parser infrastructure and normalizes output.
    """
    
    def __init__(self, source_path: Path):
        super().__init__(source_path)
    
    def load(self, payload: dict) -> CanonicalCourse:
        """Load Canvas export and convert to canonical format."""
        # Use existing ZipAdapter which handles both ZIP files and directories
        from adapters.zip_adapter import ZipAdapter
        adapter = ZipAdapter()
        canvas_course = adapter.load(payload)
        
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
        
        # Build page lookup for body content hydration
        page_map = {p.identifier: p for p in canvas_course.pages}
        
        # Build discussion lookup
        discussion_map = {d.identifier: d for d in canvas_course.discussions}
        
        # Convert modules with proper item references
        modules = []
        for canvas_module in canvas_course.modules:
            canonical_module = self._convert_module(
                canvas_module, 
                question_map,
                assessment_map,
                canvas_course.resources,
                page_map,
                discussion_map
            )
            modules.append(canonical_module)
        
        # Inject Syllabus if it exists but is not in a module
        self._inject_syllabus(canvas_course, modules)
        
        # Inject LTI External Tools
        self._inject_lti_tools(canvas_course, modules)
        
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
    
    def _convert_module(self, canvas_module, question_map, assessment_map, resources, page_map, discussion_map):
        """Convert a Canvas module."""
        items = []
        
        for item in canvas_module.items:
            content_type = self._map_content_type(item.content_type)
            
            # Build item - start with basic fields
            canonical_item = CanonicalCurriculumItem(
                identifier=item.identifier or f"item_{id(item)}",
                title=item.title,
                content_type=content_type,
                position=item.position or 0,
                source_identifier=item.identifier
            )
            
            # Hydrate body content from pages and discussions
            # Use _content_ref (resource identifierref) to look up the page
            content_ref = getattr(item, '_content_ref', None)
            if content_type == CanonicalContentType.LESSON:
                if content_ref and content_ref in page_map:
                    canonical_item.body = page_map[content_ref].body
                elif content_ref and content_ref in discussion_map:
                    canonical_item.body = discussion_map[content_ref].body
            
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

    def _inject_syllabus(self, canvas_course, modules):
        """Inject Syllabus into a special module if it exists as a separate file."""
        syllabus_path = Path(canvas_course.source_directory) / "course_settings" / "syllabus.html"
        if syllabus_path.exists():
            # Check if syllabus is already in a module
            already_in = False
            for m in modules:
                for item in m.items:
                    if "syllabus" in item.title.lower():
                        already_in = True
                        break
            
            if not already_in:
                # Create a special module for Syllabus
                try:
                    with open(syllabus_path, 'r', encoding='utf-8') as f:
                        body = f.read()
                    
                    item = CanonicalCurriculumItem(
                        identifier="syllabus_injected",
                        title="Course Syllabus",
                        content_type=CanonicalContentType.POLICY,
                        position=0,
                        body=body
                    )
                    
                    # Create or find Course Information module
                    modules.insert(0, CanonicalModule(
                        identifier="module_course_info",
                        title="Course Information",
                        items=[item],
                        position=-1
                    ))
                except Exception:
                    pass

    def _inject_lti_tools(self, canvas_course, modules):
        """Find and inject LTI tools from resources if not already in modules."""
        lti_items = []
        referenced_ids = set()
        for m in modules:
            for item in m.items:
                if item.source_identifier:
                    referenced_ids.add(item.source_identifier)
        
        for res_id, res in canvas_course.resources.items():
            if res_id not in referenced_ids and res.type and "lti" in res.type.lower():
                # This is an orphaned LTI tool
                title = res_id
                # Try to find a better title from the XML if possible
                if res.href:
                    xml_path = Path(canvas_course.source_directory) / res.href
                    if xml_path.exists():
                        try:
                            from utils.xml_utils import parse_xml_file, find_element, get_element_text
                            root = parse_xml_file(xml_path)
                            # Basic LTI uses <blti:title> or <title>
                            title_elem = root.find(".//{http://www.imsglobal.org/xsd/imsbasiclti_v1p0}title")
                            if title_elem is not None:
                                title = title_elem.text
                        except Exception:
                            pass
                
                item = CanonicalCurriculumItem(
                    identifier=f"lti_{res_id}",
                    title=title,
                    content_type=CanonicalContentType.EXTERNAL_TOOL,
                    position=len(lti_items),
                    source_identifier=res_id
                )
                lti_items.append(item)
        
        if lti_items:
            modules.append(CanonicalModule(
                identifier="module_external_tools",
                title="External Tools & Integrations",
                items=lti_items,
                position=1000
            ))
    
    # MIME type map for common course file extensions.
    # Drives correct Content-Type in S3, the asset exporter, and the frontend viewer.
    _MIME_BY_EXT: Dict[str, str] = {
        # Office
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".ppt":  "application/vnd.ms-powerpoint",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".doc":  "application/msword",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls":  "application/vnd.ms-excel",
        # Documents
        ".pdf":  "application/pdf",
        # Images
        ".png":  "image/png",
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif":  "image/gif",
        ".webp": "image/webp",
        ".svg":  "image/svg+xml",
        # Video
        ".mp4":  "video/mp4",
        ".webm": "video/webm",
        ".mov":  "video/quicktime",
        # Audio
        ".mp3":  "audio/mpeg",
        ".ogg":  "audio/ogg",
        ".wav":  "audio/wav",
        # Web
        ".html": "text/html",
        ".htm":  "text/html",
        ".xml":  "application/xml",
        ".json": "application/json",
        ".css":  "text/css",
        ".js":   "application/javascript",
        # Archives
        ".zip":  "application/zip",
    }

    @classmethod
    def _mime_from_filename(cls, filename: str) -> str:
        """Derive MIME type from file extension. Falls back to octet-stream."""
        ext = Path(filename).suffix.lower()
        return cls._MIME_BY_EXT.get(ext, "application/octet-stream")

    def _convert_assets(self, resources, source_dir):
        """Convert manifest resources to canonical assets with correct MIME types."""
        assets = []
        for res_id, res in resources.items():
            if res.href:
                filename = Path(res.href).name
                mime_type = self._mime_from_filename(filename)
                asset = CanonicalAsset(
                    identifier=res_id,
                    filename=filename,
                    source_path=res.href,
                    mime_type=mime_type,
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
            canvas_val = canvas_type.value if hasattr(canvas_type, 'value') else str(canvas_type)
            return type_map.get(canvas_val, CanonicalQuestionType.UNKNOWN)
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
            "externaltool": CanonicalContentType.EXTERNAL_TOOL,
            "lti": CanonicalContentType.EXTERNAL_TOOL,
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