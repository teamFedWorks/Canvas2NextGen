"""
Course Transformer - Maps CanvasCourse models to LmsCourse (MERN LMS) models.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..models.canvas_models import CanvasCourse, CanvasModule, CanvasModuleItem, CanvasPage, CanvasQuiz, CanvasAssignment
from ..models.lms_models import (
    LmsCourse, LmsModule, LmsLesson, LmsQuiz, LmsAssignment, 
    LmsStatus, LmsQuestion, LmsQuestionAnswer, LmsQuestionType
)
from ..models.migration_report import TransformationReport
from ..observability.logger import get_logger

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
        course_code: Optional[str] = None
    ) -> tuple[LmsCourse, TransformationReport]:
        """
        Orchestrates transformation from CanvasCourse to LmsCourse.
        """
        report = TransformationReport()
        logger.info("Starting transformation", extra={"course": canvas_course.title})

        lms_course = LmsCourse(
            title=canvas_course.title,
            description="",  # Canvas courses often don't have a root description
            status=LmsStatus.PUBLISHED,
            university=university_id,
            author_id=author_id,
            course_code=course_code,
            slug=self._slugify(canvas_course.title),
            canvas_course_id=canvas_course.identifier
        )

        # Build lookup maps for fast access to content items
        pages_map = {p.identifier: p for p in canvas_course.pages}
        quizzes_map = {q.identifier: q for q in canvas_course.quizzes}
        assignments_map = {a.identifier: a for a in canvas_course.assignments}

        # Process Modules
        for c_module in canvas_course.modules:
            lms_module = self._transform_module(c_module, pages_map, quizzes_map, assignments_map, report)
            lms_course.modules.append(lms_module)

        logger.info("Transformation complete", extra={
            "modules": len(lms_course.modules),
            "errors": len(report.errors)
        })

        return lms_course, report

    def _transform_module(
        self, 
        c_module: CanvasModule, 
        pages_map: Dict[str, CanvasPage],
        quizzes_map: Dict[str, CanvasQuiz],
        assignments_map: Dict[str, CanvasAssignment],
        report: TransformationReport
    ) -> LmsModule:
        """Transforms a Canvas module to an LMS module."""
        lms_module = LmsModule(
            title=c_module.title,
            order=c_module.position,
            canvas_id=c_module.identifier,
            description=""
        )

        # In Canvas, items are flat in a module. We categorize them for our internal LmsModule
        # (though they will be merged into a flat 'items' list in the DB later).
        for (index, item) in enumerate(c_module.items):
            if item.content_type == 'page' and item.identifier in pages_map:
                page = pages_map[item.identifier]
                lms_lesson = LmsLesson(
                    title=page.title,
                    content=page.body,
                    order=index,
                    canvas_id=page.identifier
                )
                lms_module.lessons.append(lms_lesson)
            
            elif item.content_type == 'quiz' and item.identifier in quizzes_map:
                quiz = quizzes_map[item.identifier]
                lms_quiz = self._transform_quiz(quiz, index, report)
                lms_module.quizzes.append(lms_quiz)
                
            elif item.content_type == 'assignment' and item.identifier in assignments_map:
                assign = assignments_map[item.identifier]
                lms_assign = LmsAssignment(
                    title=assign.title,
                    description=assign.description,
                    points_possible=assign.points_possible,
                    due_at=assign.due_at,
                    order=index,
                    canvas_id=assign.identifier
                )
                lms_module.assignments.append(lms_assign)

        return lms_module

    def _transform_quiz(self, c_quiz: CanvasQuiz, order: int, report: TransformationReport) -> LmsQuiz:
        """Transforms a Canvas quiz to an LMS quiz."""
        lms_quiz = LmsQuiz(
            title=c_quiz.title,
            description=c_quiz.description,
            time_limit_minutes=c_quiz.time_limit,
            attempts_allowed=c_quiz.allowed_attempts,
            order=order,
            canvas_id=c_quiz.identifier,
            shuffle_questions=c_quiz.shuffle_answers,
            show_correct_answers=c_quiz.show_correct_answers
        )

        for (q_idx, c_q) in enumerate(c_quiz.questions):
            lms_q = LmsQuestion(
                title=c_q.title,
                text=c_q.question_text,
                question_type=self._map_question_type(c_q.question_type, report),
                points=c_q.points_possible,
                order=q_idx,
                canvas_id=c_q.identifier
            )
            
            for (a_idx, c_a) in enumerate(c_q.answers):
                lms_a = LmsQuestionAnswer(
                    text=c_a.text,
                    is_correct=(c_a.weight > 0),
                    order=a_idx,
                    feedback=c_a.feedback
                )
                lms_q.answers.append(lms_a)
            
            lms_quiz.questions.append(lms_q)
            
        return lms_quiz

    def _map_question_type(self, c_type: Any, report: TransformationReport) -> LmsQuestionType:
        """Maps Canvas question types to our internal Enums with warning tracking."""
        from ..models.canvas_models import QuestionType
        
        mapping = {
            QuestionType.MULTIPLE_CHOICE: LmsQuestionType.MULTIPLE_CHOICE,
            QuestionType.TRUE_FALSE: LmsQuestionType.TRUE_FALSE,
            QuestionType.SHORT_ANSWER: LmsQuestionType.SHORT_ANSWER,
            QuestionType.ESSAY: LmsQuestionType.ESSAY,
            QuestionType.FILL_IN_BLANK: LmsQuestionType.FILL_IN_BLANK,
            QuestionType.MATCHING: LmsQuestionType.MATCHING,
            QuestionType.ORDERING: LmsQuestionType.ORDERING
        }
        
        if c_type not in mapping:
            msg = f"Unsupported Canvas QuestionType '{c_type}' fallback to 'Essay'"
            report.warnings.append(msg)
            logger.log("WARNING", msg)
            return LmsQuestionType.ESSAY
            
        return mapping[c_type]

    def _slugify(self, text: str) -> str:
        """Standard slug generator."""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        return re.sub(r'[-\s]+', '-', text).strip('-')
