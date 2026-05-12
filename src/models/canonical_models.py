"""
Canonical data models for multi-LMS ingestion.

Every LMS adapter MUST convert its platform-specific format into these models.
This is the single source of truth for all downstream processing.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class CanonicalContentType(Enum):
    """Canonical content types that work across all LMS platforms"""
    LESSON = "Lesson"
    QUIZ = "Quiz"
    ASSIGNMENT = "Assignment"
    DISCUSSION = "Discussion"
    RESOURCE = "Resource"
    WEBLINK = "WebLink"
    FILE = "File"


class CanonicalQuestionType(Enum):
    """Canonical question types - mapped from all LMS question types"""
    MULTIPLE_CHOICE = "multiple_choice"
    TRUE_FALSE = "true_false"
    FILL_BLANK = "fill_blank"
    ESSAY = "essay"
    SHORT_ANSWER = "short_answer"
    MATCHING = "matching"
    NUMERICAL = "numerical"
    FILE_UPLOAD = "file_upload"
    ORDERING = "ordering"
    CATEGORIZATION = "categorization"
    UNKNOWN = "unknown"


class SourcePlatform(Enum):
    """Supported LMS source platforms"""
    CANVAS = "canvas"
    BLACKBOARD = "blackboard"
    MOODLE = "moodle"
    D2L_BRIGHTSPACE = "d2l_brightspace"
    GOOGLE_CLASSROOM = "google_classroom"
    SCORM = "scorm"
    CUSTOM = "custom"


@dataclass
class CanonicalAsset:
    """Represents a file asset (PDF, image, video, etc.)"""
    identifier: str
    filename: str
    url: Optional[str] = None
    size_bytes: int = 0
    mime_type: str = "application/octet-stream"
    checksum: Optional[str] = None  # SHA-256 for deduplication
    
    # Source tracking
    source_path: Optional[str] = None  # Original path in export
    s3_key: Optional[str] = None


@dataclass
class CanonicalQuestion:
    """Canonical representation of an assessment question"""
    identifier: str
    text: str  # HTML content
    type: CanonicalQuestionType
    
    # Scoring
    points: float = 1.0
    
    # Answers (for supported types)
    answers: List[Dict[str, Any]] = field(default_factory=list)
    
    # Feedback
    general_feedback: Optional[str] = None
    correct_feedback: Optional[str] = None
    incorrect_feedback: Optional[str] = None
    
    # Metadata
    position: Optional[int] = None
    source_file: Optional[str] = None


@dataclass
class CanonicalAssessment:
    """Canonical representation of a quiz/assignment/exam"""
    identifier: str
    title: str
    description: str  # HTML content
    
    # Type discriminator
    is_graded: bool = True
    assessment_type: str = "quiz"  # quiz, exam, assignment
    
    # Questions
    questions: List[CanonicalQuestion] = field(default_factory=list)
    
    # Settings
    points_possible: float = 100.0
    time_limit_minutes: Optional[int] = None
    allowed_attempts: int = 1
    shuffle_answers: bool = False
    show_correct_answers: bool = True
    require_lockdown_browser: bool = False
    
    # Timing
    due_at: Optional[datetime] = None
    unlock_at: Optional[datetime] = None
    lock_at: Optional[datetime] = None


@dataclass
class CanonicalCurriculumItem:
    """A single item within a module (lesson, assessment, discussion, etc.)"""
    identifier: str
    title: str
    content_type: CanonicalContentType
    
    # Content
    body: Optional[str] = None  # HTML content for lessons/discussions
    
    # Type-specific references
    assessment_ref: Optional[str] = None  # Points to CanonicalAssessment.identifier
    asset_refs: List[str] = field(default_factory=list)  # Points to CanonicalAsset.identifier
    
    # Position within module
    position: int = 0
    
    # Metadata
    source_identifier: Optional[str] = None  # Original LMS identifier for tracing


@dataclass
class CanonicalModule:
    """A module/unit/period within a course"""
    identifier: str
    title: str
    description: str = ""
    
    # Items within this module
    items: List[CanonicalCurriculumItem] = field(default_factory=list)
    
    # Settings
    position: int = 0
    unlock_at: Optional[datetime] = None
    require_sequential_progress: bool = False
    prerequisite_module_ids: List[str] = field(default_factory=list)


@dataclass
class CanonicalCourse:
    """
    Canonical representation of a course from ANY LMS.
    
    This is the contract between the adapter layer and processing layer.
    All LMS-specific adapters MUST produce this structure.
    
    Schema versioning: Increment SCHEMA_VERSION when the canonical model changes.
    This enables backward compatibility and migration strategies.
    """
    # Identification
    identifier: str
    title: str
    source_platform: SourcePlatform
    source_course_id: Optional[str] = None  # Original LMS course ID
    
    # Schema version for evolution
    schema_version: str = "1.0"
    
    # Structure
    modules: List[CanonicalModule] = field(default_factory=list)
    
    # Assessments (flattened for easy access)
    assessments: List[CanonicalAssessment] = field(default_factory=list)
    
    # Assets (all files referenced in the course)
    assets: List[CanonicalAsset] = field(default_factory=list)
    
    # Metadata
    description: str = ""
    course_code: Optional[str] = None
    department: Optional[str] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Original source info (for debugging/tracing)
    source_directory: Optional[str] = None
    parsing_warnings: List[str] = field(default_factory=list)
    
    def get_content_counts(self) -> Dict[str, int]:
        """Get counts of all content types"""
        return {
            "modules": len(self.modules),
            "lessons": sum(
                1 for m in self.modules 
                for i in m.items 
                if i.content_type == CanonicalContentType.LESSON
            ),
            "assessments": len(self.assessments),
            "questions": sum(len(a.questions) for a in self.assessments),
            "assets": len(self.assets),
        }