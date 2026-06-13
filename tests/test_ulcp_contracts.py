import pytest
from pathlib import Path
from unittest.mock import MagicMock

from src.ucae.workflow.workspace import ExtractedWorkspace
from src.ucae.workflow.state import JobState, JobEvent
from src.ucae.validation.issue import ValidationIssue
from src.ucae.providers.base import BaseLmsProvider, ProviderMetadata, MatchResult
from src.ucae.providers.registry import ProviderRegistry
from src.ucae.workflow.registry import PipelineRegistry, PipelineStep
from src.ucae.workflow.context import PipelineContext
from src.models.canonical_models import CanonicalCourse, SourcePlatform


def test_workspace_traversal_protection(tmp_path):
    workspace = ExtractedWorkspace(tmp_path)
    
    # Valid relative path should resolve fine
    valid_file = workspace.get_file_path("valid.txt")
    assert valid_file == tmp_path / "valid.txt"

    # Traversal should raise ValueError
    with pytest.raises(ValueError, match="Path traversal detected"):
        workspace.get_file_path("../traversal.txt")


def test_provider_metadata_and_registry(tmp_path):
    workspace = ExtractedWorkspace(tmp_path)
    
    # Define a dummy provider
    class MockProvider(BaseLmsProvider):
        def __init__(self, provider_id, priority, match_version="1.0"):
            self._metadata = ProviderMetadata(
                id=provider_id,
                name=f"Mock {provider_id}",
                vendor="MockCorp",
                supported_versions=[match_version],
                priority=priority
            )
            self.match_version = match_version

        @property
        def metadata(self) -> ProviderMetadata:
            return self._metadata

        def detect(self, workspace: ExtractedWorkspace) -> MatchResult:
            if workspace.exists("marker.txt"):
                return MatchResult(matched=True, confidence=0.9, detected_version=self.match_version)
            return MatchResult(matched=False, confidence=0.0)

        def parse(self, workspace: ExtractedWorkspace):
            return {"parsed": True}

        def validate_source(self, provider_model):
            return []

        def build_canonical(self, provider_model):
            return CanonicalCourse(
                identifier="course_123",
                title="Mock Course",
                source_platform=SourcePlatform.CUSTOM
            )

    registry = ProviderRegistry()
    
    p1 = MockProvider("canvas", priority=50)
    p2 = MockProvider("blackboard", priority=100)
    
    registry.register(p1)
    registry.register(p2)
    
    # Verify priority sorting (blackboard first because priority=100 > 50)
    providers = registry.get_providers()
    assert providers[0].metadata.id == "blackboard"
    assert providers[1].metadata.id == "canvas"
    
    # Duplicate registration should raise error
    with pytest.raises(ValueError, match="already registered"):
        registry.register(MockProvider("canvas", priority=10))

    # Test detection when files do not exist
    candidates = registry.detect_provider(workspace)
    assert len(candidates) == 2
    assert not any(c.result.matched for c in candidates)
    assert candidates[0].result.confidence == 0.0

    # Write marker.txt
    marker = tmp_path / "marker.txt"
    marker.write_text("hello")
    
    # Now it should match (blackboard resolved first due to higher priority)
    candidates = registry.detect_provider(workspace)
    assert len(candidates) == 2
    assert candidates[0].result.matched is True
    assert candidates[0].provider.metadata.id == "blackboard"
    assert candidates[0].result.confidence == 0.9
    assert candidates[0].result.detected_version == "1.0"
    assert candidates[1].provider.metadata.id == "canvas"
    assert candidates[1].result.confidence == 0.9


def test_pipeline_registry_and_steps():
    registry = PipelineRegistry()
    
    class MockStep(PipelineStep):
        def __init__(self, step_name):
            self._name = step_name

        @property
        def name(self) -> str:
            return self._name

        def execute(self, context: PipelineContext) -> None:
            context.add_event(stage=self.name, message=f"Executed {self.name}")

    step1 = MockStep("extract")
    step2 = MockStep("parse")
    
    registry.register_pipeline("canvas", [step1, step2])
    
    steps = registry.get_pipeline_steps("canvas")
    assert len(steps) == 2
    assert steps[0].name == "extract"
    assert steps[1].name == "parse"
    
    with pytest.raises(KeyError):
        registry.get_pipeline_steps("unknown_provider")


def test_pipeline_context():
    context = PipelineContext(job_id="job_999")
    
    context.add_event(stage="CREATED", message="Job was created")
    context.add_metric("duration_ms", 123)
    context.add_validation_issue(
        severity="warning",
        code="BROKEN_LINK",
        path="items[0]",
        message="Broken link found",
        suggested_fix="Check link target",
        documentation_url="http://docs.example.com"
    )
    
    data = context.to_dict()
    assert data["job_id"] == "job_999"
    assert len(data["events"]) == 1
    assert data["events"][0]["stage"] == "CREATED"
    assert data["metrics"]["duration_ms"] == 123
    assert len(data["validation_issues"]) == 1
    assert data["validation_issues"][0]["code"] == "BROKEN_LINK"
    assert data["validation_issues"][0]["suggested_fix"] == "Check link target"
    assert data["validation_issues"][0]["documentation_url"] == "http://docs.example.com"
