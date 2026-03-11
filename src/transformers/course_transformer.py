"""
Course Transformer - Transforms Canvas course to Tutor LMS course.

Orchestrates all transformation logic.
"""

from typing import Dict, List
from datetime import datetime

from ..models.canvas_models import (
    CanvasCourse,
    CanvasModule,
    CanvasPage,
    CanvasAssignment,
    CanvasQuiz,
    CanvasQuestion,
    QuestionType
)
from ..models.tutor_models import (
    TutorCourse,
    TutorTopic,
    TutorLesson,
    TutorQuiz,
    TutorQuestion,
    TutorAssignment,
    TutorQuestionAnswer,
    TutorQuestionType
)
from ..models.migration_report import TransformationReport, MigrationError, ErrorSeverity
from ..config.tutor_schemas import (
    QUESTION_TYPE_MAPPING,
    CANVAS_TO_WP_STATUS,
    TUTOR_QUIZ_SETTINGS,
    TUTOR_ASSIGNMENT_SETTINGS
)
from ..utils.html_utils import rewrite_canvas_asset_paths


class CourseTransformer:
    """
    Transforms Canvas course to Tutor LMS course.
    """
    
    def __init__(self):
        """Initialize transformer"""
        self.errors: List[MigrationError] = []
        self.question_type_counts: Dict[str, int] = {}
        self.processed_assignments = set()
        self.processed_pages = set()
        self.processed_quizzes = set()
    
    def transform(self, canvas_course: CanvasCourse) -> tuple[TutorCourse, TransformationReport]:
        """
        Transform Canvas course to Tutor course.
        
        Args:
            canvas_course: Canvas course model
            
        Returns:
            Tuple of (TutorCourse, TransformationReport)
        """
        report = TransformationReport(timestamp=datetime.now())
        
        # Create Tutor course
        tutor_course = TutorCourse(
            post_title=canvas_course.title,
            post_content=f"Course: {canvas_course.title}",
            post_status=CANVAS_TO_WP_STATUS.get(canvas_course.workflow_state.value, 'publish'),
            source_canvas_course_id=canvas_course.identifier
        )
        
        # Transform modules to topics
        for module in canvas_course.modules:
            topic = self._transform_module_to_topic(module, canvas_course)
            if topic:
                tutor_course.topics.append(topic)
                report.topics_created += 1
        
        # Process orphaned assignments
        orphaned_assignments = [
            a for a in canvas_course.assignments 
            if a.identifier not in self.processed_assignments
        ]
        
        if orphaned_assignments:
            # Create a topic for orphaned assignments
            orphan_topic = TutorTopic(
                topic_title="Assignments",
                topic_order=len(tutor_course.topics) + 1,
                source_canvas_id="orphaned_assignments_topic"
            )
            
            for i, assignment in enumerate(orphaned_assignments):
                tutor_assignment = self._transform_assignment(assignment, i)
                orphan_topic.assignments.append(tutor_assignment)
            
            tutor_course.topics.append(orphan_topic)
            report.topics_created += 1
        
        # Count transformations
        for topic in tutor_course.topics:
            report.lessons_created += len(topic.lessons)
            report.quizzes_created += len(topic.quizzes)
            report.assignments_created += len(topic.assignments)
        
        # Process orphaned pages
        orphaned_pages = [
            p for p in canvas_course.pages
            if p.identifier not in self.processed_pages
        ]
        
        if orphaned_pages:
            page_topic = TutorTopic(
                topic_title="Additional Lessons",
                topic_order=len(tutor_course.topics) + 1,
                source_canvas_id="orphaned_pages_topic"
            )
            for i, page in enumerate(orphaned_pages):
                lesson = self._transform_page_to_lesson(page, i)
                page_topic.lessons.append(lesson)
            tutor_course.topics.append(page_topic)
            report.topics_created += 1
            report.lessons_created += len(page_topic.lessons)

        # Process orphaned quizzes
        orphaned_quizzes = [
            q for q in canvas_course.quizzes
            if q.identifier not in self.processed_quizzes
        ]
        
        if orphaned_quizzes:
            quiz_topic = TutorTopic(
                topic_title="Additional Quizzes",
                topic_order=len(tutor_course.topics) + 1,
                source_canvas_id="orphaned_quizzes_topic"
            )
            for i, quiz in enumerate(orphaned_quizzes):
                tutor_quiz = self._transform_quiz(quiz, i)
                quiz_topic.quizzes.append(tutor_quiz)
            tutor_course.topics.append(quiz_topic)
            report.topics_created += 1
            report.quizzes_created += len(quiz_topic.quizzes)
            for quiz in quiz_topic.quizzes:
                report.questions_created += len(quiz.questions)
            
            for quiz in topic.quizzes:
                report.questions_created += len(quiz.questions)
        
        report.question_type_mappings = self.question_type_counts
        report.errors = self.errors
        
        return tutor_course, report
    
    def _transform_module_to_topic(
        self,
        module: CanvasModule,
        canvas_course: CanvasCourse
    ) -> TutorTopic:
        """Transform Canvas module to Tutor topic"""
        topic = TutorTopic(
            topic_title=module.title,
            topic_order=module.position,
            source_canvas_id=module.identifier
        )
        
        # Transform module items
        for item in module.items:
            # Find corresponding content
            if item.content_type == 'page':
                # Find page by identifier or filename
                page = self._find_page(canvas_course, item)
                if page:
                    lesson = self._transform_page_to_lesson(page, item.position or 0)
                    topic.lessons.append(lesson)
            
            elif item.content_type == 'quiz':
                # Find quiz
                quiz = self._find_quiz(canvas_course, item)
                if quiz:
                    tutor_quiz = self._transform_quiz(quiz, item.position or 0)
                    topic.quizzes.append(tutor_quiz)
            
            elif item.content_type == 'assignment':
                # Find assignment
                assignment = self._find_assignment(canvas_course, item)
                if assignment:
                    tutor_assignment = self._transform_assignment(assignment, item.position or 0)
                    topic.assignments.append(tutor_assignment)
        
        return topic
    
    def _find_page(self, course: CanvasCourse, item) -> CanvasPage:
        """Find page by item reference"""
        # Try to match by identifier
        for page in course.pages:
            if page.identifier == item.identifier:
                return page
            # Try matching by filename
            # Try matching by filename
            if item.content_file and page.source_file:
                # Normalize separators for Windows/Unix compatibility
                item_path = item.content_file.replace('/', '\\').lower()
                page_path = page.source_file.replace('/', '\\').lower()
                
                # Check normalized substring
                if item_path in page_path:
                    return page
        return None
    
    def _find_quiz(self, course: CanvasCourse, item) -> CanvasQuiz:
        """Find quiz by item reference"""
        for quiz in course.quizzes:
            if quiz.identifier == item.identifier:
                return quiz
            if item.content_file and quiz.source_file and item.content_file in quiz.source_file:
                return quiz
        return None
    
    def _find_assignment(self, course: CanvasCourse, item) -> CanvasAssignment:
        """Find assignment by item reference"""
        for assignment in course.assignments:
            if assignment.identifier == item.identifier:
                return assignment
            if item.content_file and assignment.source_file and item.content_file in assignment.source_file:
                return assignment
        return None
    
    def _transform_page_to_lesson(self, page: CanvasPage, order: int) -> TutorLesson:
        """Transform Canvas page to Tutor lesson"""
        # Rewrite asset paths
        # Lessons are in lessons/module_X/lesson.html, so assets are in ../../assets/
        content = rewrite_canvas_asset_paths(page.body, "../../assets/")
        
        lesson = TutorLesson(
            post_title=page.title,
            post_content=content,
            post_status=CANVAS_TO_WP_STATUS.get(page.workflow_state.value, 'publish'),
            menu_order=order,
            source_canvas_id=page.identifier
        )
        
        self.processed_pages.add(page.identifier)
        
        return lesson
    
    def _transform_quiz(self, quiz: CanvasQuiz, order: int) -> TutorQuiz:
        """Transform Canvas quiz to Tutor quiz"""
        # Rewrite asset paths
        content = rewrite_canvas_asset_paths(quiz.description, "../../assets/")
        
        # Build quiz options
        quiz_option = TUTOR_QUIZ_SETTINGS.copy()
        
        if quiz.time_limit:
            quiz_option['time_limit'] = {
                'time_value': quiz.time_limit,
                'time_type': 'minutes'
            }
        
        quiz_option['attempts_allowed'] = quiz.allowed_attempts
        
        tutor_quiz = TutorQuiz(
            post_title=quiz.title,
            post_content=content,
            post_status=CANVAS_TO_WP_STATUS.get(quiz.workflow_state.value, 'publish'),
            quiz_option=quiz_option,
            menu_order=order,
            source_canvas_id=quiz.identifier
        )
        
        # Transform questions
        for position, question in enumerate(quiz.questions):
            tutor_question = self._transform_question(question, position)
            if tutor_question:
                tutor_quiz.questions.append(tutor_question)
        
        self.processed_quizzes.add(quiz.identifier)
        
        return tutor_quiz
    
    def _transform_question(self, question: CanvasQuestion, order: int) -> TutorQuestion:
        """Transform Canvas question to Tutor question"""
        # Map question type
        canvas_type = question.question_type.value
        tutor_type_str = QUESTION_TYPE_MAPPING.get(canvas_type)
        
        # Track question type usage
        self.question_type_counts[canvas_type] = self.question_type_counts.get(canvas_type, 0) + 1
        
        # If unsupported, log warning and use fallback
        if tutor_type_str is None:
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="UNSUPPORTED_QUESTION_TYPE",
                message=f"Question type '{canvas_type}' not supported, skipping question",
                canvas_entity_type="question",
                canvas_entity_id=question.identifier,
                suggested_action="Question will be skipped"
            ))
            return None
        
        # Map to TutorQuestionType enum
        try:
            tutor_type = TutorQuestionType(tutor_type_str)
        except ValueError:
            tutor_type = TutorQuestionType.OPEN_ENDED
            self.errors.append(MigrationError(
                severity=ErrorSeverity.WARNING,
                error_type="QUESTION_TYPE_FALLBACK",
                message=f"Question type '{canvas_type}' converted to open_ended (essay)",
                canvas_entity_type="question",
                canvas_entity_id=question.identifier,
                suggested_action="Manual review recommended"
            ))
        
        # Rewrite asset paths in question text
        question_text = rewrite_canvas_asset_paths(question.question_text, "../../assets/")
        
        tutor_question = TutorQuestion(
            question_title=question.title,
            question_description=question_text,
            question_type=tutor_type,
            question_mark=question.points_possible,
            question_order=order,
            source_canvas_id=question.identifier
        )
        
        # Transform answers
        for answer in question.answers:
            tutor_answer = TutorQuestionAnswer(
                answer_title=rewrite_canvas_asset_paths(answer.text, "../../assets/"),
                is_correct=(answer.weight >= 100),
                answer_order=len(tutor_question.answers)
            )
            tutor_question.answers.append(tutor_answer)
        
        return tutor_question
    
    def _transform_assignment(self, assignment: CanvasAssignment, order: int) -> TutorAssignment:
        """Transform Canvas assignment to Tutor assignment"""
        # Rewrite asset paths
        content = rewrite_canvas_asset_paths(assignment.description, "../../assets/")
        
        # Build assignment options
        assignment_option = TUTOR_ASSIGNMENT_SETTINGS.copy()
        assignment_option['total_mark'] = assignment.points_possible
        assignment_option['pass_mark'] = assignment.points_possible * 0.6  # 60% pass mark
        
        tutor_assignment = TutorAssignment(
            post_title=assignment.title,
            post_content=content,
            post_status=CANVAS_TO_WP_STATUS.get(assignment.workflow_state.value, 'publish'),
            assignment_option=assignment_option,
            menu_order=order,
            source_canvas_id=assignment.identifier
        )
        
        self.processed_assignments.add(assignment.identifier)
        
        return tutor_assignment
