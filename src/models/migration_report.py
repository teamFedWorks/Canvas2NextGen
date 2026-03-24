"""
Migration report data models.

These models represent the output reports from the migration pipeline.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class ReportStatus(Enum):
    """Overall migration status"""
    SUCCESS = "success"
    SUCCESS_WITH_WARNINGS = "success_with_warnings"
    PARTIAL_FAILURE = "partial_failure"
    FAILURE = "failure"


class ErrorSeverity(Enum):
    """Error severity levels"""
    CRITICAL = "critical"  # Pipeline stops
    ERROR = "error"  # Content lost or corrupted
    WARNING = "warning"  # Content migrated with caveats
    INFO = "info"  # Informational only


@dataclass
class MigrationError:
    """
    Represents an error or warning during migration.
    """
    severity: ErrorSeverity
    error_type: str  # PARSE_ERROR, MISSING_FILE, UNSUPPORTED_FEATURE, etc.
    message: str
    
    # Location
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    
    # Context
    canvas_entity_type: Optional[str] = None  # page, quiz, assignment, etc.
    canvas_entity_id: Optional[str] = None
    
    # Remediation
    suggested_action: Optional[str] = None
    auto_remediated: bool = False
    remediation_details: Optional[str] = None
    
    # Timestamp
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ContentInventory:
    """
    Inventory of content found in Canvas export.
    """
    # Counts
    modules: int = 0
    pages: int = 0
    assignments: int = 0
    quizzes: int = 0
    questions: int = 0
    question_banks: int = 0
    
    # Assets
    images: int = 0
    videos: int = 0
    documents: int = 0
    other_files: int = 0
    
    # Orphaned content
    orphaned_xml_files: int = 0
    orphaned_html_files: int = 0
    
    # Detailed lists
    all_files: List[str] = field(default_factory=list)
    referenced_files: List[str] = field(default_factory=list)
    orphaned_files: List[str] = field(default_factory=list)


@dataclass
class ValidationReport:
    """
    Report from Stage 1: Validation & Inventory
    """
    passed: bool
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Validation checks
    imscc_structure_valid: bool = False
    manifest_exists: bool = False
    manifest_valid_xml: bool = False
    manifest_schema_valid: bool = False
    
    # File validation
    total_referenced_files: int = 0
    missing_files: int = 0
    missing_file_list: List[str] = field(default_factory=list)
    
    # Content inventory
    inventory: ContentInventory = field(default_factory=ContentInventory)
    
    # Errors
    errors: List[MigrationError] = field(default_factory=list)


@dataclass
class ParseReport:
    """
    Report from Stage 2: Semantic Parsing
    """
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Parse success counts
    pages_parsed: int = 0
    assignments_parsed: int = 0
    quizzes_parsed: int = 0
    questions_parsed: int = 0
    
    # Parse failure counts
    pages_failed: int = 0
    assignments_failed: int = 0
    quizzes_failed: int = 0
    questions_failed: int = 0
    
    # Errors
    errors: List[MigrationError] = field(default_factory=list)


@dataclass
class ResolutionReport:
    """
    Report from Stage 3: Content Resolution
    """
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Asset resolution
    assets_resolved: int = 0
    assets_missing: int = 0
    assets_copied: int = 0
    
    # Link resolution
    internal_links_resolved: int = 0
    internal_links_broken: int = 0
    
    # Orphaned content
    orphaned_content_found: int = 0
    orphaned_content_associated: int = 0
    orphaned_content_in_recovery_module: int = 0
    
    # Deduplication
    duplicates_found: int = 0
    duplicates_merged: int = 0
    
    # Errors
    errors: List[MigrationError] = field(default_factory=list)


@dataclass
class TransformationReport:
    """
    Report from Stage 3: LMS Transformation
    """
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Transformation counts (Native LMS)
    modules_created: int = 0
    lessons_created: int = 0
    quizzes_created: int = 0
    assignments_created: int = 0
    questions_created: int = 0
    
    # Question type mappings
    question_type_mappings: Dict[str, int] = field(default_factory=dict)
    
    # Asset info
    assets_identified: int = 0
    assets_uploaded: int = 0
    
    # Errors
    errors: List[MigrationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class VerificationReport:
    """
    Report from Stage 5: Export & Verification
    """
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Integrity checks
    all_assets_exist: bool = False
    all_links_resolve: bool = False
    no_orphaned_questions: bool = False
    quiz_question_counts_match: bool = False
    module_item_counts_match: bool = False
    
    # Detailed checks
    missing_assets: List[str] = field(default_factory=list)
    broken_links: List[str] = field(default_factory=list)
    orphaned_questions: List[str] = field(default_factory=list)
    
    # Export info
    output_directory: Optional[str] = None
    output_format: str = "json"  # json, sql, wp-cli
    total_output_size_mb: float = 0.0
    
    # Errors
    errors: List[MigrationError] = field(default_factory=list)


@dataclass
class MigrationReport:
    """
    Complete migration report combining all stages.
    This is the final output report.
    """
    # Summary
    status: ReportStatus
    migration_date: datetime = field(default_factory=datetime.now)
    
    # Source and destination
    source_course_title: str = ""
    source_directory: str = ""
    output_directory: str = ""
    
    # Content summary
    source_content_counts: Dict[str, int] = field(default_factory=dict)
    migrated_content_counts: Dict[str, int] = field(default_factory=dict)
    
    # Stage reports
    validation_report: Optional[ValidationReport] = None
    parse_report: Optional[ParseReport] = None
    resolution_report: Optional[ResolutionReport] = None
    transformation_report: Optional[TransformationReport] = None
    verification_report: Optional[VerificationReport] = None
    
    # Aggregated errors
    all_errors: List[MigrationError] = field(default_factory=list)
    
    # Statistics
    total_errors: int = 0
    total_warnings: int = 0
    total_info: int = 0
    
    # Manual review items
    items_requiring_manual_review: List[Dict[str, Any]] = field(default_factory=list)
    
    # Execution time
    execution_time_seconds: float = 0.0
    
    def aggregate_errors(self):
        """Aggregate errors from all stage reports"""
        self.all_errors = []
        
        for report in [
            self.validation_report,
            self.parse_report,
            self.resolution_report,
            self.transformation_report,
            self.verification_report,
        ]:
            if report and hasattr(report, 'errors'):
                self.all_errors.extend(report.errors)
        
        # Count by severity
        self.total_errors = sum(1 for e in self.all_errors if e.severity == ErrorSeverity.ERROR or e.severity == ErrorSeverity.CRITICAL)
        self.total_warnings = sum(1 for e in self.all_errors if e.severity == ErrorSeverity.WARNING)
        self.total_info = sum(1 for e in self.all_errors if e.severity == ErrorSeverity.INFO)
        
        # Determine overall status
        if any(e.severity == ErrorSeverity.CRITICAL for e in self.all_errors):
            self.status = ReportStatus.FAILURE
        elif self.total_errors > 0:
            self.status = ReportStatus.PARTIAL_FAILURE
        elif self.total_warnings > 0:
            self.status = ReportStatus.SUCCESS_WITH_WARNINGS
        else:
            self.status = ReportStatus.SUCCESS
    
    def get_summary_dict(self) -> Dict[str, Any]:
        """Get a summary dictionary for JSON export"""
        return {
            "status": self.status.value,
            "migration_date": self.migration_date.isoformat(),
            "source_course": self.source_course_title,
            "source_directory": self.source_directory,
            "output_directory": self.output_directory,
            "content_migrated": self.migrated_content_counts,
            "errors": self.total_errors,
            "warnings": self.total_warnings,
            "execution_time_seconds": self.execution_time_seconds,
        }
