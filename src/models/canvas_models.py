"""
Typed data models for Canvas LMS entities.

These models represent the Canvas course structure as parsed from IMS-CC exports.
All models use dataclasses for type safety and validation.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Literal
from enum import Enum
from datetime import datetime


class WorkflowState(Enum):
    """Canvas workflow states"""
    ACTIVE = "active"
    UNPUBLISHED = "unpublished"
    DELETED = "deleted"


class QuestionType(Enum):
    """Canvas question types (QTI-compliant)"""
    MULTIPLE_CHOICE = "multiple_choice_question"
    TRUE_FALSE = "true_false_question"
    FILL_IN_BLANK = "fill_in_multiple_blanks_question"
    ESSAY = "essay_question"
    SHORT_ANSWER = "short_answer_question"
    MATCHING = "matching_question"
    NUMERICAL = "numerical_question"
    CALCULATED = "calculated_question"
    MULTIPLE_ANSWERS = "multiple_answers_question"
    FILE_UPLOAD = "file_upload_question"
    TEXT_ONLY = "text_only_question"
    MULTIPLE_DROPDOWNS = "multiple_dropdowns_question"
    # Advanced types
    FORMULA = "formula_question"
    CATEGORIZATION = "categorization_question"
    ORDERING = "ordering_question"


class SubmissionType(Enum):
    """Canvas assignment submission types"""
    ONLINE_TEXT_ENTRY = "online_text_entry"
    ONLINE_URL = "online_url"
    ONLINE_UPLOAD = "online_upload"
    ONLINE_QUIZ = "online_quiz"
    MEDIA_RECORDING = "media_recording"
    EXTERNAL_TOOL = "external_tool"
    NONE = "none"


@dataclass
class CanvasResource:
    """
    Represents a resource reference from imsmanifest.xml
    """
    identifier: str
    href: Optional[str]
    type: str
    title: Optional[str] = None
    
    # Nested files within this resource
    files: List[str] = field(default_factory=list)
    
    # Validation flag
    file_exists: bool = False
    resolved_path: Optional[str] = None


@dataclass
class CanvasModuleItem:
    """
    Represents an item within a Canvas module.
    Can be a page, assignment, quiz, discussion, weblink, or nested module.
    """
    title: str
    identifier: Optional[str] = None
    content_type: Optional[str] = None  # 'page', 'assignment', 'quiz', 'discussion', 'weblink'
    content_file: Optional[str] = None
    indent: int = 0
    
    # The resource identifierref from the manifest — used by the transformer
    # to look up the correct parsed content object (page/quiz/assignment/etc.)
    _content_ref: Optional[str] = field(default=None, metadata={"export": False})
    
    # Nested items (for sub-modules)
    items: List['CanvasModuleItem'] = field(default_factory=list)
    
    # Metadata
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    position: Optional[int] = None


@dataclass
class CanvasModule:
    """
    Represents a Canvas module (organizational container).
    """
    title: str
    identifier: Optional[str] = None
    position: int = 0
    
    # Module items (lessons, quizzes, assignments)
    items: List[CanvasModuleItem] = field(default_factory=list)
    
    # Module settings
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    unlock_at: Optional[datetime] = None
    require_sequential_progress: bool = False
    
    # Prerequisites
    prerequisite_module_ids: List[str] = field(default_factory=list)


@dataclass
class CanvasPage:
    """
    Represents a Canvas wiki page.
    """
    title: str
    identifier: str
    body: str  # HTML content
    
    # Metadata
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    editing_roles: str = "teachers"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Source file reference
    source_file: Optional[str] = None


@dataclass
class CanvasDiscussion:
    """
    Represents a Canvas discussion topic.
    """
    title: str
    identifier: str
    body: str  # HTML content
    
    # Metadata
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    
    # Source file reference (the XML configuration)
    source_file: Optional[str] = None


@dataclass
class CanvasWebLink:
    """
    Represents a Canvas external URL resource.
    """
    title: str
    identifier: str
    url: str
    
    # Source file reference (the XML configuration)
    source_file: Optional[str] = None


@dataclass
class CanvasRubricCriterion:
    """Rubric criterion for assignment grading"""
    description: str
    points: float
    criterion_id: str
    ratings: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CanvasAssignment:
    """
    Represents a Canvas assignment.
    """
    title: str
    identifier: str
    description: str  # HTML content
    
    # Grading
    points_possible: float = 0.0
    grading_type: str = "points"  # points, percent, letter_grade, gpa_scale
    
    # Submission
    submission_types: List[SubmissionType] = field(default_factory=list)
    allowed_extensions: List[str] = field(default_factory=list)
    
    # Timing
    due_at: Optional[datetime] = None
    unlock_at: Optional[datetime] = None
    lock_at: Optional[datetime] = None
    
    # Settings
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    position: Optional[int] = None
    
    # Rubric
    rubric: List[CanvasRubricCriterion] = field(default_factory=list)
    
    # Source file reference
    source_file: Optional[str] = None


@dataclass
class CanvasQuestionAnswer:
    """
    Represents a possible answer for a Canvas question.
    """
    id: str
    text: str  # HTML content
    weight: float = 0.0  # 100 = correct, 0 = incorrect
    feedback: Optional[str] = None  # HTML feedback
    
    # For matching questions
    match_id: Optional[str] = None


@dataclass
class CanvasQuestion:
    """
    Represents a Canvas quiz question.
    """
    identifier: str
    title: str
    question_type: QuestionType
    question_text: str  # HTML content
    
    # Grading
    points_possible: float = 1.0
    
    # Answers
    answers: List[CanvasQuestionAnswer] = field(default_factory=list)
    
    # Feedback
    general_feedback: Optional[str] = None
    correct_feedback: Optional[str] = None
    incorrect_feedback: Optional[str] = None
    neutral_feedback: Optional[str] = None
    
    # Metadata
    position: Optional[int] = None
    
    # For numerical/calculated questions
    tolerance: Optional[float] = None
    formula: Optional[str] = None
    
    # For fill-in-blank questions
    blank_id: Optional[str] = None
    
    # Source file reference
    source_file: Optional[str] = None


@dataclass
class CanvasQuestionGroup:
    """
    Represents a question group (pick N questions from a bank).
    """
    identifier: str
    title: str
    pick_count: int  # Number of questions to pick
    points_per_question: float = 1.0
    
    # Questions in this group
    questions: List[CanvasQuestion] = field(default_factory=list)


@dataclass
class CanvasQuiz:
    """
    Represents a Canvas quiz/assessment.
    """
    title: str
    identifier: str
    description: str  # HTML content
    
    # Quiz settings
    quiz_type: str = "assignment"  # assignment, practice_quiz, graded_survey, survey
    points_possible: float = 0.0
    time_limit: Optional[int] = None  # Minutes
    allowed_attempts: int = 1
    scoring_policy: str = "keep_highest"  # keep_highest, keep_latest
    
    # Questions
    questions: List[CanvasQuestion] = field(default_factory=list)
    question_groups: List[CanvasQuestionGroup] = field(default_factory=list)
    
    # Timing
    due_at: Optional[datetime] = None
    unlock_at: Optional[datetime] = None
    lock_at: Optional[datetime] = None
    
    # Settings
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    shuffle_answers: bool = False
    show_correct_answers: bool = True
    show_correct_answers_at: Optional[datetime] = None
    hide_correct_answers_at: Optional[datetime] = None
    
    # Proctoring / browser lockdown
    # Parsed directly from <require_lockdown_browser> in assessment_meta.xml.
    # True means Respondus LockDown Browser is required to take this quiz.
    # The quiz questions ARE fully exported — only the browser enforcement
    # setting needs to be configured manually in the target LMS.
    require_lockdown_browser: bool = False
    require_lockdown_browser_for_results: bool = False
    
    # Access
    access_code: Optional[str] = None
    ip_filter: Optional[str] = None
    
    # Source file reference
    source_file: Optional[str] = None


@dataclass
class CanvasQuestionBank:
    """
    Represents a Canvas question bank.
    """
    identifier: str
    title: str
    
    # Questions in this bank
    questions: List[CanvasQuestion] = field(default_factory=list)
    
    # Source file reference
    source_file: Optional[str] = None


@dataclass
class CanvasCourse:
    """
    Represents the complete Canvas course structure.
    This is the root data model for the parsed Canvas export.
    """
    title: str
    identifier: Optional[str] = None
    
    # Course structure
    modules: List[CanvasModule] = field(default_factory=list)
    
    # Content
    pages: List[CanvasPage] = field(default_factory=list)
    assignments: List[CanvasAssignment] = field(default_factory=list)
    quizzes: List[CanvasQuiz] = field(default_factory=list)
    discussions: List[CanvasDiscussion] = field(default_factory=list)
    weblinks: List[CanvasWebLink] = field(default_factory=list)
    question_banks: List[CanvasQuestionBank] = field(default_factory=list)
    
    # Resources (from manifest)
    resources: Dict[str, CanvasResource] = field(default_factory=dict)
    
    # Metadata
    created_at: Optional[datetime] = None
    workflow_state: WorkflowState = WorkflowState.ACTIVE
    
    # Source directory
    source_directory: Optional[str] = None
    
    def get_content_counts(self) -> Dict[str, int]:
        """Get counts of all content types"""
        return {
            "modules": len(self.modules),
            "pages": len(self.pages),
            "assignments": len(self.assignments),
            "quizzes": len(self.quizzes),
            "questions": sum(len(q.questions) for q in self.quizzes),
            "question_banks": len(self.question_banks),
        }
    
    def get_all_questions(self) -> List[CanvasQuestion]:
        """Get all questions from all quizzes and banks"""
        questions = []
        for quiz in self.quizzes:
            questions.extend(quiz.questions)
            for group in quiz.question_groups:
                questions.extend(group.questions)
        for bank in self.question_banks:
            questions.extend(bank.questions)
        return questions
