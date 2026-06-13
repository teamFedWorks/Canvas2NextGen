from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
from enum import Enum

from src.ucae.workflow.workspace import ExtractedWorkspace
from src.ucae.providers.base import ProviderMetadata
from src.ucae.validation.issue import ValidationIssue
from src.models.canonical_models import (
    CanonicalCourse, 
    CanonicalModule, 
    CanonicalCurriculumItem, 
    CanonicalAssessment, 
    CanonicalQuestion, 
    CanonicalAsset,
    SourcePlatform,
    CanonicalContentType,
    CanonicalQuestionType
)
from src.ucae.workflow.state import JobEvent


def serialize_enum_default(obj: Any) -> Any:
    """Helper to serialize Enum objects to their underlying values for clean JSON."""
    if isinstance(obj, Enum):
        return obj.value
    return str(obj)


@dataclass
class PipelineContext:
    """
    PipelineContext carries the state of a single course ingestion execution run.
    It holds file references to serialized artifacts (on disk) instead of raw 
    large memory structures to prevent high resident memory usage during enterprise migrations.
    """
    job_id: str
    workspace: Optional[ExtractedWorkspace] = None
    provider_metadata: Optional[ProviderMetadata] = None
    persistence: Optional[Any] = None  # Receives JobPersistenceService to auto-persist events
    
    # File reference paths (relative to workspace root)
    provider_model_ref: Optional[str] = None
    canonical_course_ref: Optional[str] = None
    normalized_canonical_course_ref: Optional[str] = None
    import_manifest_ref: Optional[str] = None
    import_result_ref: Optional[str] = None
    
    # In-memory logs, metrics, and structured issues
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    events: List[JobEvent] = field(default_factory=list)
    logger: Optional[logging.Logger] = None

    def add_event(self, stage: str, message: str, actor: str = "system", metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a state transition event in the context.
        Appends to local events and forwards to persistent DB if configured.
        """
        event = JobEvent(
            stage=stage,
            message=message,
            actor=actor,
            metadata=metadata or {}
        )
        self.events.append(event)
        
        if self.logger:
            self.logger.info(f"Job {self.job_id} [{stage}] ({actor}): {message}")

        if self.persistence:
            try:
                self.persistence.log_event(
                    job_id=self.job_id,
                    stage=stage,
                    message=message,
                    actor=actor,
                    details=metadata
                )
            except Exception:
                # Do not block processing if event-log DB write fails
                pass

    def add_metric(self, name: str, value: Any) -> None:
        """Sets or updates a metric for tracking performance and telemetry."""
        self.metrics[name] = value

    def add_validation_issue(
        self, 
        severity: str, 
        code: str, 
        path: str, 
        message: str,
        suggested_fix: Optional[str] = None,
        documentation_url: Optional[str] = None
    ) -> None:
        """Utility to add a structured validation issue directly, auto-populating provider context."""
        provider_id = self.provider_metadata.id if self.provider_metadata else None
        provider_ver = None
        if self.provider_metadata and self.provider_metadata.supported_versions:
            provider_ver = self.provider_metadata.supported_versions[0]

        self.validation_issues.append(
            ValidationIssue(
                severity=severity,
                code=code,
                path=path,
                message=message,
                provider=provider_id,
                provider_version=provider_ver,
                suggested_fix=suggested_fix,
                documentation_url=documentation_url
            )
        )

    # --- Disk-Based Serialization / Deserialization Helpers ---

    def save_provider_model(self, model_dict: Dict[str, Any]) -> str:
        """Serializes the provider model dict to disk, setting the context reference."""
        if not self.workspace:
            raise ValueError("No workspace bound to context.")
        
        ref_path = f"artifacts/{self.job_id}_provider_model.json"
        full_path = self.workspace.get_file_path(ref_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(model_dict, f, default=serialize_enum_default)
            
        self.provider_model_ref = ref_path
        self.artifacts[ref_path] = "provider_model"
        return ref_path

    def load_provider_model(self) -> Dict[str, Any]:
        """Loads and returns the serialized provider model from disk."""
        if not self.workspace or not self.provider_model_ref:
            raise ValueError("No workspace or provider model reference configured.")
        
        full_path = self.workspace.get_file_path(self.provider_model_ref)
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_canonical_course(self, course: CanonicalCourse, is_normalized: bool = False) -> str:
        """Serializes CanonicalCourse object to disk, setting the corresponding reference."""
        if not self.workspace:
            raise ValueError("No workspace bound to context.")
        
        suffix = "normalized" if is_normalized else "canonical"
        ref_path = f"artifacts/{self.job_id}_{suffix}_course.json"
        full_path = self.workspace.get_file_path(ref_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(asdict(course), f, default=serialize_enum_default)
            
        if is_normalized:
            self.normalized_canonical_course_ref = ref_path
            self.artifacts[ref_path] = "normalized_canonical_course"
        else:
            self.canonical_course_ref = ref_path
            self.artifacts[ref_path] = "canonical_course"
            
        return ref_path

    def load_canonical_course(self, is_normalized: bool = False) -> CanonicalCourse:
        """Loads and reconstructs the CanonicalCourse object from the disk reference."""
        if not self.workspace:
            raise ValueError("No workspace bound to context.")
            
        ref = self.normalized_canonical_course_ref if is_normalized else self.canonical_course_ref
        if not ref:
            raise ValueError(f"No {'normalized' if is_normalized else 'canonical'} course reference configured.")
            
        full_path = self.workspace.get_file_path(ref)
        with open(full_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return self._dict_to_course(data)

    def save_import_manifest(self, manifest_dict: Dict[str, Any]) -> str:
        """Serializes the import manifest report to disk, setting the context reference."""
        if not self.workspace:
            raise ValueError("No workspace bound to context.")
            
        ref_path = f"artifacts/{self.job_id}_import_manifest.json"
        full_path = self.workspace.get_file_path(ref_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(manifest_dict, f, default=serialize_enum_default)
            
        self.import_manifest_ref = ref_path
        self.artifacts[ref_path] = "import_manifest"
        return ref_path

    def load_import_manifest(self) -> Dict[str, Any]:
        """Loads and returns the serialized import manifest dictionary from disk."""
        if not self.workspace or not self.import_manifest_ref:
            raise ValueError("No workspace or import manifest reference configured.")
            
        full_path = self.workspace.get_file_path(self.import_manifest_ref)
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the context state for DB storage/logging."""
        return {
            "job_id": self.job_id,
            "provider_metadata": {
                "id": self.provider_metadata.id,
                "name": self.provider_metadata.name,
                "vendor": self.provider_metadata.vendor
            } if self.provider_metadata else None,
            "provider_model_ref": self.provider_model_ref,
            "canonical_course_ref": self.canonical_course_ref,
            "normalized_canonical_course_ref": self.normalized_canonical_course_ref,
            "import_manifest_ref": self.import_manifest_ref,
            "import_result_ref": self.import_result_ref,
            "metrics": self.metrics,
            "artifacts": list(self.artifacts.keys()),
            "validation_issues": [issue.to_dict() for issue in self.validation_issues],
            "events": [event.to_dict() for event in self.events]
        }

    # --- Internal Reconstructor Helper ---

    def _dict_to_course(self, d: Dict[str, Any]) -> CanonicalCourse:
        """Reconstructs CanonicalCourse type-safely from deserialized JSON dictionaries."""
        
        def parse_dt(s: Optional[str]) -> Optional[datetime]:
            if not s or s == "None":
                return None
            try:
                return datetime.fromisoformat(s)
            except ValueError:
                return None

        # Reconstruct assets
        assets = []
        for a in d.get("assets", []):
            assets.append(CanonicalAsset(**a))

        # Reconstruct assessments
        assessments = []
        for val in d.get("assessments", []):
            questions = []
            for q in val.get("questions", []):
                q_copy = dict(q)
                if "type" in q_copy and isinstance(q_copy["type"], str):
                    q_copy["type"] = CanonicalQuestionType(q_copy["type"])
                questions.append(CanonicalQuestion(**q_copy))
            val_copy = dict(val)
            val_copy["questions"] = questions
            val_copy["due_at"] = parse_dt(val_copy.get("due_at"))
            val_copy["unlock_at"] = parse_dt(val_copy.get("unlock_at"))
            val_copy["lock_at"] = parse_dt(val_copy.get("lock_at"))
            assessments.append(CanonicalAssessment(**val_copy))

        # Reconstruct modules
        modules = []
        for m in d.get("modules", []):
            items = []
            for i in m.get("items", []):
                i_copy = dict(i)
                if "content_type" in i_copy and isinstance(i_copy["content_type"], str):
                    i_copy["content_type"] = CanonicalContentType(i_copy["content_type"])
                items.append(CanonicalCurriculumItem(**i_copy))
            m_copy = dict(m)
            m_copy["items"] = items
            m_copy["unlock_at"] = parse_dt(m_copy.get("unlock_at"))
            modules.append(CanonicalModule(**m_copy))

        return CanonicalCourse(
            identifier=d["identifier"],
            title=d["title"],
            source_platform=SourcePlatform(d["source_platform"]),
            source_course_id=d.get("source_course_id"),
            schema_version=d.get("schema_version", "1.0"),
            modules=modules,
            assessments=assessments,
            assets=assets,
            description=d.get("description", ""),
            course_code=d.get("course_code"),
            department=d.get("department"),
            created_at=parse_dt(d.get("created_at")),
            updated_at=parse_dt(d.get("updated_at")),
            source_directory=d.get("source_directory"),
            parsing_warnings=d.get("parsing_warnings", [])
        )
