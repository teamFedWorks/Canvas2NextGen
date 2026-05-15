"""
Custom LMS Domain Models (MERN-Aligned)

Typed dataclasses for the custom MERN-based LMS MongoDB schema.
Aligned with the required JSON structure provided by the user.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class LmsStatus(Enum):
    """Content publication status."""
    PUBLISHED = "Published"
    DRAFT = "Draft"
    ARCHIVED = "Archived"


class LmsItemType(Enum):
    """Types of curriculum items."""
    LESSON = "Lesson"
    QUIZ = "Quiz"
    ASSIGNMENT = "Assignment"


# ---------------------------------------------------------------------------
# Nested Configurations
# ---------------------------------------------------------------------------

@dataclass
class LmsPricing:
    model: str = "PerCredit"
    amount: float = 0.0
    currency: str = "USD"


@dataclass
class LmsFlags:
    isFeatured: bool = False
    isVerified: bool = False
    isBestSeller: bool = False
    allowGuestPreview: bool = True
    requiresApprovalToEnroll: bool = False


@dataclass
class LmsStats:
    totalStudents: int = 0
    averageRating: float = 0.0
    reviewsCount: int = 0
    completionRate: float = 0.0


@dataclass
class LmsSettings:
    isPublished: bool = True
    isFreePreview: bool = False
    isDownloadable: bool = True
    isPrerequisite: bool = False


@dataclass
class LmsGradeSettings:
    isGraded: bool = True
    maxScore: float = 100.0
    passingScore: Optional[float] = None


@dataclass
class LmsAssignmentConfig:
    gradeSettings: LmsGradeSettings = field(default_factory=LmsGradeSettings)
    fileUploadLimit: int = 1
    maxFileSizeMB: int = 10
    maxResubmissions: int = 3
    type: str = "Individual"


@dataclass
class LmsQuizConfig:
    gradeSettings: LmsGradeSettings = field(default_factory=LmsGradeSettings)
    timeLimit: int = 60
    attemptsAllowed: int = 1
    showResultsOnFinish: bool = True
    showCorrectAnswers: bool = False
    # Respondus LockDown Browser requirement — read from assessment_meta.xml.
    # True means students must have Respondus installed to take this quiz.
    # The quiz questions are fully imported; only this browser setting needs
    # manual configuration in the target LMS.
    requireLockdownBrowser: bool = False
    requireLockdownBrowserForResults: bool = False


@dataclass
class LmsAttachment:
    name: str
    url: str
    size: str = "0MB"
    type: str = "UNKNOWN"


# ---------------------------------------------------------------------------
# Quiz Question Models
# ---------------------------------------------------------------------------

@dataclass
class LmsQuestionAnswer:
    """A single answer choice for a quiz question."""
    id: str
    text: str
    isCorrect: bool = False
    feedback: Optional[str] = None


@dataclass
class LmsQuestion:
    """
    A quiz question stored under a curriculum item.
    Mapped from CanvasQuestion during transformation.
    """
    identifier: str
    text: str          # HTML question prompt
    type: str          # multiple_choice, true_false, essay, short_answer, etc.
    points: float = 1.0
    answers: List[LmsQuestionAnswer] = field(default_factory=list)
    generalFeedback: Optional[str] = None
    position: Optional[int] = None


# ---------------------------------------------------------------------------
# Curriculum Items
# ---------------------------------------------------------------------------

@dataclass
class LmsCurriculumItem:
    """
    Unified item for Lesson, Quiz, or Assignment.
    """
    title: str
    slug: str
    type: str  # Lesson, Quiz, Assignment
    settings: LmsSettings = field(default_factory=LmsSettings)
    content: str = ""
    attachments: List[LmsAttachment] = field(default_factory=list)

    # Explicit ordering field — prevents silent reordering when MongoDB
    # returns documents in a different sequence.
    position: int = 0

    # Optional configs based on type
    quizConfig: Optional[LmsQuizConfig] = None
    assignmentConfig: Optional[LmsAssignmentConfig] = None

    # Quiz questions — populated for Quiz items during transformation.
    # Stored inline under the course document (Phase 1 fix).
    questions: List[LmsQuestion] = field(default_factory=list)

    # Traceability (not in target JSON but kept for internal use)
    _canvasId: Optional[str] = field(default=None, metadata={"export": False})
    _content_ref: Optional[str] = field(default=None, metadata={"export": False})


@dataclass
class LmsCurriculumModule:
    """
    Represents a course module (e.g., Week 1).
    """
    title: str
    summary: str = ""
    locked: bool = False
    isVisible: bool = True
    isPublished: bool = True
    settings: Dict[str, bool] = field(default_factory=lambda: {"isLocked": False, "isOptional": False})
    items: List[LmsCurriculumItem] = field(default_factory=list)
    
    # Traceability
    _canvasId: Optional[str] = field(default=None, metadata={"export": False})


# ---------------------------------------------------------------------------
# Root Course Model
# ---------------------------------------------------------------------------

@dataclass
class LmsCourse:
    """
    Root document for a custom LMS course.
    Mappings aligned with MERN backend schema.
    """
    # Core Identity
    university: str                        # ObjectId string
    title: str
    slug: str
    courseUrl: str
    authorId: str                          # ObjectId string
    courseCode: Optional[str] = None
    
    # Metadata
    department: str = "Unknown"
    credits: int = 3
    semester: str = "Year-Round"
    academicYear: str = "2026-2027"
    description: str = ""
    shortDescription: str = ""
    featuredImage: str = "https://placehold.co/600x400?text=Course+Image"
    
    categories: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    difficultyLevel: str = "All Levels"
    language: str = "English"
    
    # Authorship
    authorName: str = "Admin SFC"
    
    # Financials
    pricing: LmsPricing = field(default_factory=LmsPricing)
    isPaid: bool = True
    
    # Access
    status: str = "Published"
    isPublic: bool = True
    flags: LmsFlags = field(default_factory=LmsFlags)
    
    # Counters
    stats: LmsStats = field(default_factory=LmsStats)
    enrollmentCount: int = 0
    applicantsCount: int = 0
    
    # Hierarchy
    curriculum: List[LmsCurriculumModule] = field(default_factory=list)
    
    # Timestamps
    createdAt: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    updatedAt: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

    # Source traceability (kept for idempotency, will be filtered if needed)
    canvas_course_id: Optional[str] = None

    def get_content_counts(self) -> Dict[str, int]:
        """Return counts of all content types for reporting."""
        modules = len(self.curriculum)
        lessons = sum(len(m.items) for m in self.curriculum if hasattr(m, 'items'))
        assessments = len([item for m in self.curriculum for item in (m.items if hasattr(m, 'items') else []) if getattr(item, 'type', None) == 'quiz'])
        questions = 0
        for m in self.curriculum:
            if hasattr(m, 'items'):
                for item in m.items:
                    if hasattr(item, 'type') and item.type == 'quiz' and hasattr(item, 'questions'):
                        questions += len(item.questions)
        assets = 0
        for m in self.curriculum:
            if hasattr(m, 'items'):
                for item in m.items:
                    if hasattr(item, 'assets'):
                        assets += len(item.assets)
                    elif hasattr(item, 'asset_refs'):
                        assets += len(item.asset_refs)
        return {
            "modules": modules,
            "lessons": lessons,
            "assessments": assessments,
            "questions": questions,
            "assets": assets,
        }
