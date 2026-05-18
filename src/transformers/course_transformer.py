"""
Course Transformer - Maps CanvasCourse models to LmsCourse (MERN LMS) models.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from models.canvas_models import CanvasCourse, CanvasModule, CanvasModuleItem, CanvasPage, CanvasQuiz, CanvasAssignment, CanvasDiscussion, CanvasWebLink
from models.lms_models import (
    LmsCourse, LmsCurriculumModule, LmsCurriculumItem, LmsQuizConfig, 
    LmsAssignmentConfig, LmsGradeSettings, LmsStatus, LmsItemType,
    LmsQuestion, LmsQuestionAnswer
)
from models.migration_report import TransformationReport
from observability.logger import get_logger
from config.lms_schemas import CANVAS_QUESTION_TYPE_MAP

logger = get_logger(__name__)


class CourseTransformer:
    """
    Transforms parsed Canvas data into the custom LMS domain models.
    Ensures alignment with the backend's nested database schema.
    """

    def transform(
        self, 
        canvas_course: CanvasCourse, 
        university_id: Optional[str] = None, 
        author_id: Optional[str] = None,
        course_code: Optional[str] = None,
        department: Optional[str] = None
    ) -> tuple[LmsCourse, TransformationReport]:
        """
        Orchestrates transformation from CanvasCourse to LmsCourse.
        """
        report = TransformationReport()
        logger.info("[CourseTransformer] Starting transformation", extra={"course": canvas_course.title})

        slug = self._slugify(canvas_course.title)
        import os
        lms_course = LmsCourse(
            university=university_id or os.getenv("DEFAULT_UNIVERSITY_ID", "000000000000000000000000"),
            authorId=author_id or os.getenv("DEFAULT_AUTHOR_ID", "000000000000000000000000"),
            authorName="Admin",
            title=canvas_course.title,
            slug=slug,
            courseUrl=slug,
            courseCode=course_code or "IMPORTED",
            department=department or "Imported",
            shortDescription=f"{canvas_course.title} — imported from Canvas LMS",
            description=f"Imported Course: {canvas_course.title}",
            canvas_course_id=canvas_course.identifier
        )

        # Build lookup maps for fast access to content items.
        # Key = identifier used when the content was parsed.
        # Pages are keyed by their stem (filename without extension) from wiki_content/
        # Quizzes and assignments are keyed by their directory name.
        # Discussions and weblinks are keyed by their resource identifier (set in parser.py).
        pages_map = {p.identifier: p for p in canvas_course.pages}
        quizzes_map = {q.identifier: q for q in canvas_course.quizzes}
        assignments_map = {a.identifier: a for a in canvas_course.assignments}
        discussions_map = {d.identifier: d for d in canvas_course.discussions}
        weblinks_map = {w.identifier: w for w in canvas_course.weblinks}

        # Process Modules
        for c_module in canvas_course.modules:
            lms_module = self._transform_module(
                c_module, pages_map, quizzes_map, assignments_map,
                discussions_map, weblinks_map, report
            )
            lms_course.curriculum.append(lms_module)

        logger.info("[CourseTransformer] Transformation complete", extra={
            "modules": len(lms_course.curriculum),
            "errors": len(report.errors)
        })

        return lms_course, report

    def _transform_module(
        self, 
        c_module: CanvasModule, 
        pages_map: Dict[str, CanvasPage],
        quizzes_map: Dict[str, CanvasQuiz],
        assignments_map: Dict[str, CanvasAssignment],
        discussions_map: Dict[str, CanvasDiscussion],
        weblinks_map: Dict[str, CanvasWebLink],
        report: TransformationReport
    ) -> LmsCurriculumModule:
        """Transforms a Canvas module to a curriculum module."""
        lms_module = LmsCurriculumModule(
            title=c_module.title,
            summary="",
            _canvasId=c_module.identifier
        )

        for item in c_module.items:
            lms_item = self._transform_item(
                item, pages_map, quizzes_map, assignments_map,
                discussions_map, weblinks_map, report
            )
            if lms_item:
                lms_module.items.append(lms_item)

        return lms_module

    def _transform_item(
        self, 
        c_item: CanvasModuleItem,
        pages_map: Dict[str, CanvasPage],
        quizzes_map: Dict[str, CanvasQuiz],
        assignments_map: Dict[str, CanvasAssignment],
        discussions_map: Dict[str, CanvasDiscussion],
        weblinks_map: Dict[str, CanvasWebLink],
        report: TransformationReport
    ) -> Optional[LmsCurriculumItem]:
        """Maps a Canvas module item to a unified LmsCurriculumItem."""
        base_item = LmsCurriculumItem(
            title=c_item.title,
            slug=self._slugify(c_item.title),
            _canvasId=c_item.identifier,
            type="Lesson"
        )

        # Use _content_ref (the resource identifierref) for content lookup.
        # Fall back to identifier for backwards compatibility.
        lookup_key = getattr(c_item, '_content_ref', None) or c_item.identifier

        # Carry the content_ref onto the LMS item so AssetUploader can match it
        base_item._content_ref = lookup_key

        if c_item.content_type == 'page':
            page = pages_map.get(lookup_key)
            
            # Semantic tagging based on title
            title_lower = (c_item.title or "").lower()
            if any(kw in title_lower for kw in ["syllabus", "policy", "rules", "guideline", "honor code"]):
                base_item.type = "Policy"
            elif any(kw in title_lower for kw in ["resource", "support", "help", "tutorial", "uploading", "guide"]):
                base_item.type = "Resource"
            elif any(kw in title_lower for kw in ["textbook", "reading", "chapter"]):
                base_item.type = "Reading"
            elif any(kw in title_lower for kw in ["zoom", "webinar", "live session"]):
                base_item.type = "LiveSession"
            elif any(kw in title_lower for kw in ["announcement", "welcome note"]):
                base_item.type = "Announcement"
            elif any(kw in title_lower for kw in ["survey", "evaluation", "feedback"]):
                base_item.type = "Survey"
            else:
                base_item.type = "Lesson"

            if page:
                base_item.content = page.body
            # Always return — even without body, the item exists in the module
            # and the AssetUploader will attach any file resources to it.
            return base_item

        elif c_item.content_type == 'quiz':
            quiz = quizzes_map.get(lookup_key)
            if quiz:
                base_item.type = "Quiz"
                # Use description if available; otherwise generate a minimal placeholder
                # so the item isn't flagged as empty by the validator
                if quiz.description and quiz.description.strip():
                    base_item.content = quiz.description
                elif quiz.questions:
                    base_item.content = f"<p>Quiz: {quiz.title} — {len(quiz.questions)} question(s)</p>"
                else:
                    base_item.content = f"<p>Quiz: {quiz.title}</p>"

                # Persist parsed quiz questions (previously discarded).
                mapped_questions: List[LmsQuestion] = []
                for cq in (quiz.questions or []):
                    canvas_qt = getattr(cq, "question_type", None)
                    canvas_qt_value = canvas_qt.value if canvas_qt else None
                    lms_qt = CANVAS_QUESTION_TYPE_MAP.get(canvas_qt_value)
                    if lms_qt is None:
                        # Not renderable in our LMS.
                        continue

                    answers: List[LmsQuestionAnswer] = []
                    for ca in (cq.answers or []):
                        weight = getattr(ca, "weight", 0.0) or 0.0
                        answers.append(
                            LmsQuestionAnswer(
                                id=getattr(ca, "id", ""),
                                text=getattr(ca, "text", "") or "",
                                isCorrect=(float(weight) >= 100.0),
                                feedback=getattr(ca, "feedback", None),
                            )
                        )

                    mapped_questions.append(
                        LmsQuestion(
                            identifier=getattr(cq, "identifier", ""),
                            text=getattr(cq, "question_text", "") or "",
                            type=lms_qt,
                            points=float(getattr(cq, "points_possible", 1.0) or 1.0),
                            answers=answers,
                            generalFeedback=getattr(cq, "general_feedback", None),
                            position=getattr(cq, "position", None),
                        )
                    )

                base_item.questions = mapped_questions
                base_item.quizConfig = LmsQuizConfig(
                    gradeSettings=LmsGradeSettings(maxScore=quiz.points_possible or 100.0),
                    timeLimit=quiz.time_limit or 60,
                    attemptsAllowed=quiz.allowed_attempts,
                    showCorrectAnswers=quiz.show_correct_answers,
                    requireLockdownBrowser=quiz.require_lockdown_browser,
                    requireLockdownBrowserForResults=quiz.require_lockdown_browser_for_results,
                )
                return base_item
            # Quiz declared in manifest but not parsed — keep as placeholder
            base_item.type = "Quiz"
            return base_item

        elif c_item.content_type == 'assignment':
            assign = assignments_map.get(lookup_key)
            if assign:
                base_item.type = "Assignment"
                base_item.content = assign.description
                base_item.assignmentConfig = LmsAssignmentConfig(
                    gradeSettings=LmsGradeSettings(maxScore=assign.points_possible or 100.0),
                    type="Individual"
                )
                return base_item
            # Assignment declared but not parsed — keep as placeholder
            base_item.type = "Assignment"
            return base_item

        elif c_item.content_type == 'discussion':
            discussion = discussions_map.get(lookup_key)
            base_item.type = "Lesson"
            if discussion:
                base_item.content = discussion.body
            return base_item

        elif c_item.content_type == 'weblink':
            weblink = weblinks_map.get(lookup_key)
            base_item.type = "Lesson"
            if weblink:
                base_item.content = (
                    f'<p><a href="{weblink.url}" target="_blank" rel="noopener noreferrer">'
                    f'{weblink.title}</a></p>'
                )
            return base_item

        # Unknown / unresolved content type — keep as empty lesson so the
        # module item is not silently dropped from the course structure.
        return base_item

    def _slugify(self, text: str) -> str:
        """Standard slug generator."""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        return re.sub(r'[-\s]+', '-', text).strip('-')
