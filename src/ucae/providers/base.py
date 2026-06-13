from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from src.ucae.workflow.workspace import ExtractedWorkspace
from src.ucae.validation.issue import ValidationIssue
from src.models.canonical_models import CanonicalCourse


@dataclass
class ProviderMetadata:
    id: str
    name: str
    vendor: str
    supported_versions: List[str]
    priority: int = 100
    capabilities: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchResult:
    matched: bool
    confidence: float  # 0.0 to 1.0 indicating confidence of detection
    detected_version: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class BaseLmsProvider(ABC):
    """
    Abstract base class for all LMS provider adapters (Canvas, Blackboard, Moodle, etc.).
    Providers are decoupled from execution and reporting. They are only responsible for
    detecting, parsing, and transforming their specific package formats.
    """
    
    @property
    @abstractmethod
    def metadata(self) -> ProviderMetadata:
        """Exposes platform identifier, version support, priority, and capabilities."""
        pass

    @abstractmethod
    def detect(self, workspace: ExtractedWorkspace) -> MatchResult:
        """
        Scans the extracted workspace structure to detect if this provider fits
        and returns detection confidence and version.
        """
        pass

    @abstractmethod
    def parse(self, workspace: ExtractedWorkspace) -> Any:
        """
        Reads files from the workspace and parses them into provider-specific model classes.
        """
        pass

    @abstractmethod
    def validate_source(self, provider_model: Any) -> List[ValidationIssue]:
        """
        Runs pre-ingestion source-specific format sanity checks (e.g. malformed XML, missing manifests).
        """
        pass

    @abstractmethod
    def build_canonical(self, provider_model: Any) -> CanonicalCourse:
        """
        Transforms the platform-specific course model into the vendor-neutral CanonicalCourse model.
        """
        pass
