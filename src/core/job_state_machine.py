"""
Job State Machine - Persistent, resumable ingestion workflow state.

This tracks every ingestion job through its lifecycle and enables:
- Checkpoint recovery
- Partial retries
- Distributed worker coordination
- Audit trail
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid


class JobState(Enum):
    """Finite state machine for ingestion jobs."""
    RECEIVED = "received"           # Job just created
    CLASSIFYING = "classifying"     # LMS detection in progress
    CLASSIFIED = "classified"       # Classification complete
    VALIDATING = "validating"       # Schema validation
    VALIDATED = "validated"         # Validation passed
    RESOLVING = "resolving"         # Manifest dependency resolution
    RESOLVED = "resolved"           # Dependencies mapped
    PARSING = "parsing"             # Converting to canonical
    PARSED = "parsed"               # Parsing complete
    ENRICHING = "enriching"         # Content enrichment
    ENRICHED = "enriched"           # Enrichment complete
    UPLOADING_ASSETS = "uploading_assets"  # S3 asset migration
    ASSETS_UPLOADED = "assets_uploaded"
    EXPORTING = "exporting"         # Persistence
    COMPLETED = "completed"         # Success
    FAILED = "failed"               # Terminal failure
    RETRYING = "retrying"           # Temporary retry state
    CANCELLED = "cancelled"         # User cancelled


@dataclass
class JobCheckpoint:
    """Checkpoint for resumability."""
    stage: JobState
    timestamp: datetime
    progress_pct: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    artifact_paths: List[str] = field(default_factory=list)


@dataclass
class JobRetryPolicy:
    """Retry configuration for a job."""
    max_attempts: int = 3
    backoff_multiplier: float = 2.0
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    retry_on_states: List[JobState] = field(default_factory=lambda: [
        JobState.RETRYING,
        JobState.PARSING,
        JobState.UPLOADING_ASSETS,
    ])


@dataclass
class IngestionJob:
    """
    Persistent job state for an ingestion workflow.
    
    Every transition is logged and checkpointed.
    This object is the source of truth for recovery.
    """
    
    # Identity
    job_id: str
    correlation_id: str  # For distributed tracing
    task_id: Optional[str] = None  # Legacy compatibility
    
    # Current state
    state: JobState = JobState.RECEIVED
    current_stage: str = ""
    
    # Input
    source_type: str = "zip"  # zip, s3, canvas_api, etc.
    source_path: Optional[str] = None
    source_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Classification
    platform: Optional[str] = None
    platform_confidence: float = 0.0
    platform_version: Optional[str] = None
    
    # Validation
    validation_passed: bool = False
    validation_errors: List[Dict[str, Any]] = field(default_factory=list)
    
    # Progress
    progress_pct: int = 0
    progress_message: str = ""
    
    # Checkpoints
    checkpoints: List[JobCheckpoint] = field(default_factory=list)
    
    # Artifacts (paths to intermediate results)
    extract_dir: Optional[str] = None
    canonical_path: Optional[str] = None
    manifest_graph_path: Optional[str] = None
    
    # Error handling
    error_count: int = 0
    last_error: Optional[str] = None
    retry_count: int = 0
    retry_policy: JobRetryPolicy = field(default_factory=JobRetryPolicy)
    
    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Result
    course_id: Optional[str] = None
    output_paths: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for storage."""
        return {
            "job_id": self.job_id,
            "correlation_id": self.correlation_id,
            "task_id": self.task_id,
            "state": self.state.value,
            "current_stage": self.current_stage,
            "source_type": self.source_type,
            "source_path": self.source_path,
            "source_metadata": self.source_metadata,
            "platform": self.platform,
            "platform_confidence": self.platform_confidence,
            "validation_passed": self.validation_passed,
            "validation_errors": self.validation_errors,
            "progress_pct": self.progress_pct,
            "progress_message": self.progress_message,
            "checkpoints": [
                {
                    "stage": cp.stage.value,
                    "timestamp": cp.timestamp.isoformat(),
                    "progress_pct": cp.progress_pct,
                    "metadata": cp.metadata,
                    "artifact_paths": cp.artifact_paths
                }
                for cp in self.checkpoints
            ],
            "extract_dir": self.extract_dir,
            "canonical_path": self.canonical_path,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "course_id": self.course_id,
            "output_paths": self.output_paths,
            "warnings": self.warnings,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IngestionJob':
        """Deserialize from storage."""
        job = cls(
            job_id=data["job_id"],
            correlation_id=data.get("correlation_id", str(uuid.uuid4())),
            task_id=data.get("task_id"),
            state=JobState(data["state"]),
            current_stage=data.get("current_stage", ""),
            source_type=data.get("source_type", "zip"),
            source_path=data.get("source_path"),
            source_metadata=data.get("source_metadata", {}),
            platform=data.get("platform"),
            platform_confidence=data.get("platform_confidence", 0.0),
            validation_passed=data.get("validation_passed", False),
            validation_errors=data.get("validation_errors", []),
            progress_pct=data.get("progress_pct", 0),
            progress_message=data.get("progress_message", ""),
            error_count=data.get("error_count", 0),
            last_error=data.get("last_error"),
            retry_count=data.get("retry_count", 0),
        )
        
        if "created_at" in data:
            job.created_at = datetime.fromisoformat(data["created_at"])
        if "started_at" in data and data["started_at"]:
            job.started_at = datetime.fromisoformat(data["started_at"])
        if "completed_at" in data and data["completed_at"]:
            job.completed_at = datetime.fromisoformat(data["completed_at"])
        
        # Reconstruct checkpoints
        for cp_data in data.get("checkpoints", []):
            cp = JobCheckpoint(
                stage=JobState(cp_data["stage"]),
                timestamp=datetime.fromisoformat(cp_data["timestamp"]),
                progress_pct=cp_data["progress_pct"],
                metadata=cp_data.get("metadata", {}),
                artifact_paths=cp_data.get("artifact_paths", [])
            )
            job.checkpoints.append(cp)
        
        job.extract_dir = data.get("extract_dir")
        job.canonical_path = data.get("canonical_path")
        job.course_id = data.get("course_id")
        job.output_paths = data.get("output_paths", [])
        job.warnings = data.get("warnings", [])
        
        return job
    
    def checkpoint(self, stage: JobState, progress: int, message: str, 
                   metadata: Dict[str, Any] = None, artifacts: List[str] = None):
        """Record a checkpoint."""
        cp = JobCheckpoint(
            stage=stage,
            timestamp=datetime.utcnow(),
            progress_pct=progress,
            metadata=metadata or {},
            artifact_paths=artifacts or []
        )
        self.checkpoints.append(cp)
        self.state = stage
        self.progress_pct = progress
        self.progress_message = message
    
    def is_retryable(self) -> bool:
        """Check if job can be retried."""
        return (
            self.state in self.retry_policy.retry_on_states
            and self.retry_count < self.retry_policy.max_attempts
        )
    
    def get_elapsed_seconds(self) -> float:
        """Get total elapsed time."""
        start = self.started_at or self.created_at
        end = self.completed_at or datetime.utcnow()
        return (end - start).total_seconds()