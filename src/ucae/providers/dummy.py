import json
from datetime import datetime, timezone
from typing import Any, List

from src.ucae.providers.base import BaseLmsProvider, ProviderMetadata, MatchResult
from src.ucae.validation.issue import ValidationIssue
from src.ucae.workflow.workspace import ExtractedWorkspace
from src.models.canonical_models import (
    CanonicalCourse,
    CanonicalModule,
    CanonicalCurriculumItem,
    CanonicalAssessment,
    CanonicalQuestion,
    CanonicalQuestionType,
    CanonicalContentType,
    SourcePlatform
)


class DummyProvider(BaseLmsProvider):
    """
    Deterministic Dummy provider for end-to-end integration testing.
    Loads mock course payload and outputs identical canonical outputs for snapshot validation.
    """
    
    @property
    def metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            id="dummy",
            name="Dummy Ingestion Platform",
            vendor="ULCP-Internal",
            supported_versions=["1.0"],
            priority=1
        )

    def detect(self, workspace: ExtractedWorkspace) -> MatchResult:
        """Looks for the dummy package indicator file."""
        if workspace.exists("dummy/manifest.json"):
            return MatchResult(matched=True, confidence=1.0, detected_version="1.0")
        return MatchResult(matched=False, confidence=0.0)

    def parse(self, workspace: ExtractedWorkspace) -> Any:
        """Parses manifest, lesson content, and quiz details into raw dictionaries."""
        # 1. Parse manifest
        manifest_path = workspace.get_file_path("dummy/manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        # 2. Parse lesson body
        lesson_file = manifest["lessons"][0]
        lesson_path = workspace.get_file_path(lesson_file)
        with open(lesson_path, "r", encoding="utf-8") as f:
            lesson_body = f.read()

        # 3. Parse quiz
        quiz_file = manifest["quiz"]
        quiz_path = workspace.get_file_path(quiz_file)
        with open(quiz_path, "r", encoding="utf-8") as f:
            quiz = json.load(f)

        return {
            "title": manifest["title"],
            "identifier": manifest["identifier"],
            "lesson_body": lesson_body,
            "quiz": quiz
        }

    def validate_source(self, provider_model: Any) -> List[ValidationIssue]:
        """Runs basic source structure validation checks."""
        issues = []
        if not provider_model.get("title"):
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="MISSING_TITLE",
                    path="manifest.title",
                    message="Course title is missing from dummy manifest."
                )
            )
        return issues

    def build_canonical(self, provider_model: Any) -> CanonicalCourse:
        """Transforms mock dictionaries into a completely deterministic CanonicalCourse."""
        # Fixed epoch timestamp for 100% byte-identical snapshot outputs
        fixed_time = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)

        # Build quiz questions
        questions = []
        for index, q in enumerate(provider_model["quiz"]["questions"]):
            questions.append(
                CanonicalQuestion(
                    identifier=f"q_{index}",
                    text=q["text"],
                    type=CanonicalQuestionType.MULTIPLE_CHOICE,
                    points=q.get("points", 1.0),
                    answers=q["answers"],
                    position=index
                )
            )

        # Build assessment
        assessment = CanonicalAssessment(
            identifier="assessment_quiz",
            title=provider_model["quiz"]["title"],
            description=provider_model["quiz"]["description"],
            questions=questions,
            due_at=fixed_time
        )

        # Build lesson curriculum item
        lesson_item = CanonicalCurriculumItem(
            identifier="lesson_1",
            title="Lesson 1",
            content_type=CanonicalContentType.LESSON,
            body=provider_model["lesson_body"],
            position=0
        )

        # Build learning module
        module = CanonicalModule(
            identifier="module_1",
            title="Module 1",
            items=[lesson_item],
            position=0
        )

        return CanonicalCourse(
            identifier=provider_model["identifier"],
            title=provider_model["title"],
            source_platform=SourcePlatform.CUSTOM,
            schema_version="1.0",
            modules=[module],
            assessments=[assessment],
            created_at=fixed_time,
            updated_at=fixed_time
        )
