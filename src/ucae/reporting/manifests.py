from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional


@dataclass
class ImportManifest:
    """
    Structured manifest describing the ingested learning package content.
    Used for audit trails and target LMS sync, replacing raw dictionary logging.
    """
    course_title: str
    source_platform: str
    schema_version: str
    ingested_at: datetime = field(default_factory=datetime.utcnow)
    content_counts: Dict[str, int] = field(default_factory=dict)
    asset_checksums: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "course_title": self.course_title,
            "source_platform": self.source_platform,
            "schema_version": self.schema_version,
            "ingested_at": self.ingested_at.isoformat(),
            "content_counts": self.content_counts,
            "asset_checksums": self.asset_checksums,
            "metadata": self.metadata
        }


@dataclass
class ImportResult:
    """
    Standardized result payload returned upon completing an ingestion execution run.
    """
    job_id: str
    status: str                        # "success" | "failed" | "warning"
    duration_seconds: float
    error_message: Optional[str] = None
    validation_summary: Dict[str, int] = field(default_factory=dict)  # e.g. {"errors": 0, "warnings": 2}
    manifest: Optional[ImportManifest] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "validation_summary": self.validation_summary,
            "manifest": self.manifest.to_dict() if self.manifest else None
        }
