import pytest
from datetime import datetime
from src.ucae.canonical.normalizer import CanonicalNormalizer
from src.ucae.canonical.assets import AssetRegistry
from src.ucae.reporting.manifests import ImportManifest, ImportResult
from src.models.canonical_models import CanonicalCourse, CanonicalModule, CanonicalAsset, SourcePlatform


def test_canonical_normalizer():
    # Setup CanonicalCourse with volatile timestamps and unsorted list fields
    course = CanonicalCourse(
        identifier="course_123",
        title="  Intro to AIâ€”Section 1  ",  # with mojibake
        source_platform=SourcePlatform.CANVAS,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        assets=[
            CanonicalAsset(identifier="asset_B", filename="B.pdf"),
            CanonicalAsset(identifier="asset_A", filename="A.pdf")
        ],
        modules=[
            CanonicalModule(
                identifier="mod_1",
                title="Module 1",
                prerequisite_module_ids=["mod_3", "mod_2"]
            )
        ]
    )

    normalizer = CanonicalNormalizer()
    normalized = normalizer.normalize(course)

    # 1. Title encoding repaired and stripped
    assert normalized.title == "Intro to AI-Section 1"
    
    # 2. Volatile timestamps cleaned (set to None)
    assert normalized.created_at is None
    assert normalized.updated_at is None

    # 3. Assets list sorted by identifier
    assert normalized.assets[0].identifier == "asset_A"
    assert normalized.assets[1].identifier == "asset_B"

    # 4. Prerequisite modules sorted
    assert normalized.modules[0].prerequisite_module_ids == ["mod_2", "mod_3"]

    # 5. Course immutability: check original course has not been mutated
    assert course.title == "  Intro to AIâ€”Section 1  "
    assert course.created_at is not None


def test_asset_registry():
    registry = AssetRegistry()
    
    # Non-existent asset should return None
    assert registry.get_asset("hash_123") is None

    # Reserve asset
    status, asset = registry.reserve_asset(checksum="hash_123", worker_id="worker_A")
    assert status == "RESERVED"
    assert asset is None

    # Complete upload
    asset = registry.complete_upload(
        checksum="hash_123",
        s3_key="s3://bucket/file.pdf",
        cdn_url="https://cdn/file.pdf",
        size_bytes=1024,
        mime_type="application/pdf"
    )

    assert asset.checksum == "hash_123"
    
    # Fetch registered asset
    fetched = registry.get_asset("hash_123")
    assert fetched is not None
    assert fetched.s3_key == "s3://bucket/file.pdf"
    assert fetched.cdn_url == "https://cdn/file.pdf"

    # Reserve again - should return COMPLETED
    status2, asset2 = registry.reserve_asset(checksum="hash_123", worker_id="worker_B")
    assert status2 == "COMPLETED"
    assert asset2 is not None
    assert asset2.s3_key == "s3://bucket/file.pdf"


def test_import_reporting_serialization():
    manifest = ImportManifest(
        course_title="AI Intro",
        source_platform="canvas",
        schema_version="1.0",
        content_counts={"modules": 1, "lessons": 5},
        asset_checksums=["hash_1"]
    )

    result = ImportResult(
        job_id="job_abc",
        status="success",
        duration_seconds=1.5,
        validation_summary={"errors": 0, "warnings": 1},
        manifest=manifest
    )

    data = result.to_dict()
    assert data["job_id"] == "job_abc"
    assert data["status"] == "success"
    assert data["duration_seconds"] == 1.5
    assert data["validation_summary"]["warnings"] == 1
    assert data["manifest"]["course_title"] == "AI Intro"
    assert data["manifest"]["content_counts"]["lessons"] == 5


def test_fingerprint_versioning():
    course = CanonicalCourse(identifier="course_123", title="AI 101", source_platform=SourcePlatform.CANVAS)
    normalizer_v1 = CanonicalNormalizer(version="1.0")
    normalizer_v2 = CanonicalNormalizer(version="2.0")
    
    n_v1 = normalizer_v1.normalize(course)
    n_v2 = normalizer_v2.normalize(course)

    fp_v1 = normalizer_v1.compute_content_fingerprint(n_v1)
    fp_v2 = normalizer_v2.compute_content_fingerprint(n_v2)

    # Fingerprints must be deterministic for the same version
    assert fp_v1 == normalizer_v1.compute_content_fingerprint(n_v1)
    # Fingerprints must be different across normalizer versions to avoid collision/stale state
    assert fp_v1 != fp_v2


def test_recovery_artifact_compression():
    from src.ucae.workflow.recovery import RecoveryArtifact
    payload = {"modules_count": 14, "course_code": "CS101", "large_field": "A" * 1000}
    
    artifact = RecoveryArtifact.serialize(
        payload_dict=payload,
        schema_version="2.0",
        provider_version="2025"
    )

    assert artifact.schema_version == "2.0"
    assert artifact.provider_version == "2025"
    assert artifact.serialization_version == "1.0"
    # Payload must be compressed (base64 string representing gzipped data)
    assert isinstance(artifact.compressed_payload, str)

    # Deserialize back
    recovered = artifact.deserialize()
    assert recovered["course_code"] == "CS101"
    assert recovered["modules_count"] == 14
    assert recovered["large_field"] == "A" * 1000
