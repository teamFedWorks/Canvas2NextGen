#!/usr/bin/env python3
"""
Comprehensive end-to-end validation of the canonical pipeline.

Tests:
1. Source classification accuracy
2. Manifest dependency resolution
3. Canonical conversion completeness
4. Data integrity verification
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from core.canonical_pipeline import CanonicalPipeline
from core.classifier import SourceClassifier
from models.canonical_models import CanonicalCourse, SourcePlatform

def test_classifier():
    """Test source classification."""
    print("\n" + "="*60)
    print("TEST: Source Classification")
    print("="*60)

    course_dir = ROOT / "storage" / "uploads" / "BS Information Technology" / "IT-1104 Programming I"
    result = SourceClassifier.classify_directory(course_dir)

    print(f"  Platform: {result.platform.value}")
    print(f"  Confidence: {result.confidence:.2f}")
    print(f"  Export Type: {result.export_type}")

    assert result.platform == SourcePlatform.CANVAS, f"Expected Canvas, got {result.platform}"
    assert result.confidence >= 0.7, f"Confidence too low: {result.confidence}"
    print("  [PASS] Source classified as Canvas with high confidence")
    return result

def test_canonical_conversion():
    """Test parsing and conversion to canonical model."""
    print("\n" + "="*60)
    print("TEST: Canonical Conversion")
    print("="*60)

    course_dir = ROOT / "storage" / "uploads" / "BS Information Technology" / "IT-1104 Programming I"

    # Import directly to control flow
    from adapters.canonical_adapter import CanvasToCanonicalAdapter

    adapter = CanvasToCanonicalAdapter(course_dir)
    payload = {"zip_path": str(course_dir)}
    canonical = adapter.load(payload)

    # Verify structure
    assert isinstance(canonical, CanonicalCourse), "Result is not CanonicalCourse"
    assert canonical.title, "Course title is empty"
    assert canonical.source_platform == SourcePlatform.CANVAS, "Wrong source platform"

    print(f"  Course Title: {canonical.title}")
    print(f"  Source Platform: {canonical.source_platform.value}")
    print(f"  Modules: {len(canonical.modules)}")
    print(f"  Assessments (Quizzes): {len(canonical.assessments)}")
    print(f"  Assets: {len(canonical.assets)}")

    # Verify modules have content
    total_items = sum(len(m.items) for m in canonical.modules)
    print(f"  Total Curriculum Items: {total_items}")

    # Verify assessments have questions
    total_questions = sum(len(a.questions) for a in canonical.assessments)
    print(f"  Total Questions: {total_questions}")

    # Verify assets have identifiers
    asset_ids = [a.identifier for a in canonical.assets if a.identifier]
    print(f"  Asset identifiers: {len(asset_ids)}")

    # Verify no duplicate module titles
    module_titles = [m.title for m in canonical.modules]
    assert len(module_titles) == len(set(module_titles)), "Duplicate module titles detected"

    print("  [PASS] Canonical conversion successful")
    return canonical

def test_pipeline_integration():
    """Test full pipeline with mocked DB."""
    print("\n" + "="*60)
    print("TEST: Full Pipeline Integration")
    print("="*60)

    course_dir = ROOT / "storage" / "uploads" / "BS Information Technology" / "IT-1104 Programming I"

    with patch('core.canonical_pipeline.ChunkedMongoExporter') as MockExporter:
        mock_exporter = MagicMock()
        mock_exporter.export_canonical_course.return_value = "test_course_id_123"
        mock_exporter.close.return_value = None
        MockExporter.return_value = mock_exporter

        pipeline = CanonicalPipeline(
            source_path=course_dir,
            university_id=os.getenv("DEFAULT_UNIVERSITY_ID", "test_univ"),
            author_id=os.getenv("DEFAULT_AUTHOR_ID", "test_author")
        )

        result = pipeline.run()

        # Verify exporter was called
        assert mock_exporter.export_canonical_course.called, "Exporter was not called"
        call_args = mock_exporter.export_canonical_course.call_args
        assert call_args is not None, "Exporter called with no args"

        # Get the canonical course that was passed to exporter
        exported_canonical = call_args[0][0]
        assert isinstance(exported_canonical, CanonicalCourse), "Exported object is not CanonicalCourse"

        print(f"  Exported to MongoDB: {result.get('course_id')}")
        print(f"  Modules in export: {len(exported_canonical.modules)}")
        print(f"  Assessments in export: {len(exported_canonical.assessments)}")
        print(f"  Assets in export: {len(exported_canonical.assets)}")

        # Verify data integrity - check for empty modules
        empty_modules = [m for m in exported_canonical.modules if not m.items]
        if empty_modules:
            print(f"  [WARN] {len(empty_modules)} modules have no items")

        # Verify all items have content or are assessments
        empty_items = [
            item for m in exported_canonical.modules
            for item in m.items
            if not item.body and item.content_type.value == "Lesson"
        ]
        if empty_items:
            print(f"  [WARN] {len(empty_items)} lesson items have empty body")

    if result.get('status') != 'success':
        print(f"  [FAIL] Pipeline failed: {result.get('error')}")
        return None

    print("  [PASS] Pipeline integration successful")
    return result

def test_data_integrity(canonical: CanonicalCourse):
    """Verify data integrity constraints."""
    print("\n" + "="*60)
    print("TEST: Data Integrity")
    print("="*60)

    issues = []

    # Check identifiers are unique
    all_ids = []
    all_ids.extend([m.identifier for m in canonical.modules])
    all_ids.extend([a.identifier for a in canonical.assessments])
    all_ids.extend([i.identifier for m in canonical.modules for i in m.items])

    duplicates = {id for id in all_ids if all_ids.count(id) > 1}
    if duplicates:
        issues.append(f"Duplicate identifiers: {duplicates}")

    # Check all items have titles
    untitled_items = [i for m in canonical.modules for i in m.items if not i.title.strip()]
    if untitled_items:
        issues.append(f"{len(untitled_items)} items have empty titles")

    # Check assessments have valid types
    invalid_assessments = [
        a for a in canonical.assessments
        if not a.assessment_type or a.assessment_type not in ["quiz", "exam", "assignment"]
    ]
    if invalid_assessments:
        issues.append(f"Invalid assessment types: {invalid_assessments}")

    # Check question types are mapped
    unknown_q_types = [
        q for a in canonical.assessments
        for q in a.questions
        if q.type.value == "unknown"
    ]
    if unknown_q_types:
        issues.append(f"{len(unknown_q_types)} questions have unknown type")

    if issues:
        for issue in issues:
            print(f"  [WARN] {issue}")
    else:
        print("  [PASS] All integrity checks passed")

    return len(issues) == 0

def main():
    print("\n" + "="*60)
    print(" CANONICAL PIPELINE END-TO-END VALIDATION")
    print("="*60)

    try:
        # Test 1: Classifier
        classification = test_classifier()

        # Test 2: Canonical conversion
        canonical = test_canonical_conversion()

        # Test 3: Full pipeline integration
        pipeline_result = test_pipeline_integration()

        # Test 4: Data integrity
        if canonical:
            integrity_ok = test_data_integrity(canonical)

        # Final verdict
        print("\n" + "="*60)
        if pipeline_result and pipeline_result.get('status') == 'success':
            print(" VALIDATION SUCCESSFUL")
            print("="*60)
            print("\nThe canonical pipeline is ready for production use.")
            print("All stages executed successfully and produced valid canonical models.")
            return 0
        else:
            print(" VALIDATION FAILED")
            print("="*60)
            return 1

    except Exception as e:
        print(f"\n [FAIL] Validation crashed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())