import sys
from pathlib import Path
import json
import zipfile

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from ucae.workflow.workspace import ExtractedWorkspace
from ucae.workflow.state import JobState, JobEvent
from ucae.providers.dummy import DummyProvider
from ucae.providers.registry import ProviderRegistry
from ucae.canonical.normalizer import CanonicalNormalizer
from ucae.worker.listener import IngestionQueueListener
from ucae.workflow.input_source import LocalFileInputSource


# In-memory MongoDB Mock classes for isolation and speed


# In-memory MongoDB Mock classes for isolation and speed

def _match_doc(doc, query, now=None):
    if now is None:
        import datetime
        now = datetime.datetime.utcnow()
    for k, v in query.items():
        if k == "$or":
            if not any(_match_doc(doc, subq, now) for subq in v):
                return False
        elif k == "$and":
            if not all(_match_doc(doc, subq, now) for subq in v):
                return False
        else:
            doc_val = doc.get(k)
            if isinstance(v, dict):
                for op, op_val in v.items():
                    if op == "$lt":
                        if doc_val is None or doc_val >= op_val:
                            return False
                    elif op == "$gt":
                        if doc_val is None or doc_val <= op_val:
                            return False
                    elif op == "$ne":
                        if doc_val == op_val:
                            return False
                    elif op == "$in":
                        if doc_val not in op_val:
                            return False
                    else:
                        return False
            else:
                if doc_val != v:
                    return False
    return True

def _apply_update(doc, update, is_insert=False):
    if "$set" in update:
        for k, v in update["$set"].items():
            doc[k] = v
    if "$unset" in update:
        for k in update["$unset"]:
            doc.pop(k, None)
    if "$setOnInsert" in update and is_insert:
        for k, v in update["$setOnInsert"].items():
            doc[k] = v
    if "$push" in update:
        for k, v in update["$push"].items():
            if k not in doc:
                doc[k] = []
            doc[k].append(v)

class MockCollection:
    def __init__(self, name=""):
        self.name = name
        self.data = {}
        self._indexes = {
            "_id_": {"key": [("_id", 1)]}
        }
        if name == "jobs":
            self._indexes["job_id_1"] = {"key": [("job_id", 1)], "unique": True}
        elif name == "locks":
            self._indexes["expires_at_1"] = {"key": [("expires_at", 1)], "expireAfterSeconds": 300}
        elif name == "assets":
            self._indexes["expiresAt_1"] = {"key": [("expiresAt", 1)], "expireAfterSeconds": 300}
        elif name == "worker_heartbeats":
            self._indexes["lastHeartbeat_1"] = {"key": [("lastHeartbeat", 1)], "expireAfterSeconds": 600}

    def index_information(self):
        return self._indexes

    def find_one(self, query, *args, **kwargs):
        for doc in self.data.values():
            if _match_doc(doc, query):
                return doc
        return None

    def insert_one(self, doc, *args, **kwargs):
        key = doc.get("_id")
        if not key:
            if self.name == "jobs" and doc.get("job_id"):
                key = doc.get("job_id")
            elif self.name == "assets" and doc.get("checksum"):
                key = doc.get("checksum")
            else:
                import uuid
                key = str(uuid.uuid4())
            doc["_id"] = key
        
        if key in self.data:
            from pymongo.errors import DuplicateKeyError
            raise DuplicateKeyError(f"Duplicate key error: {key}")
            
        self.data[key] = dict(doc)
        return MagicMock(inserted_id=key)

    def delete_one(self, query, *args, **kwargs):
        to_del = None
        for key, doc in self.data.items():
            if _match_doc(doc, query):
                to_del = key
                break
        if to_del:
            del self.data[to_del]
            return MagicMock(deleted_count=1)
        return MagicMock(deleted_count=0)

    def update_one(self, query, update, upsert=False, *args, **kwargs):
        matched_key = None
        for key, doc in self.data.items():
            if _match_doc(doc, query):
                matched_key = key
                break
        
        if matched_key:
            _apply_update(self.data[matched_key], update, is_insert=False)
            return MagicMock(modified_count=1, matched_count=1)
        
        if upsert:
            new_doc = {}
            for k, v in query.items():
                if not k.startswith("$") and not isinstance(v, dict):
                    new_doc[k] = v
            if "_id" not in new_doc and "_id" in query and not isinstance(query["_id"], dict):
                new_doc["_id"] = query["_id"]
                
            _apply_update(new_doc, update, is_insert=True)
            key = new_doc.get("_id")
            if not key:
                if self.name == "jobs" and new_doc.get("job_id"):
                    key = new_doc.get("job_id")
                elif self.name == "assets" and new_doc.get("checksum"):
                    key = new_doc.get("checksum")
                else:
                    import uuid
                    key = str(uuid.uuid4())
                new_doc["_id"] = key
            self.data[key] = new_doc
            return MagicMock(modified_count=1, matched_count=0, upserted_id=key)
            
        return MagicMock(modified_count=0, matched_count=0)

    def find_one_and_update(self, query, update, upsert=False, return_document=False, *args, **kwargs):
        matched_key = None
        for key, doc in self.data.items():
            if _match_doc(doc, query):
                matched_key = key
                break
        
        if matched_key:
            old_doc = dict(self.data[matched_key])
            _apply_update(self.data[matched_key], update, is_insert=False)
            return self.data[matched_key] if return_document else old_doc
            
        if upsert:
            new_doc = {}
            for k, v in query.items():
                if not k.startswith("$") and not isinstance(v, dict):
                    new_doc[k] = v
            if "_id" not in new_doc and "_id" in query and not isinstance(query["_id"], dict):
                new_doc["_id"] = query["_id"]
            if "checksum" not in new_doc and "checksum" in query and not isinstance(query["checksum"], dict):
                new_doc["checksum"] = query["checksum"]
                
            _apply_update(new_doc, update, is_insert=True)
            key = new_doc.get("_id")
            if not key:
                if self.name == "jobs" and new_doc.get("job_id"):
                    key = new_doc.get("job_id")
                elif self.name == "assets" and new_doc.get("checksum"):
                    key = new_doc.get("checksum")
                else:
                    import uuid
                    key = str(uuid.uuid4())
                new_doc["_id"] = key
            self.data[key] = new_doc
            return new_doc
            
        return None

    def replace_one(self, query, doc, upsert=False, *args, **kwargs):
        matched_key = None
        for key, existing_doc in self.data.items():
            if _match_doc(existing_doc, query):
                matched_key = key
                break
                
        if matched_key:
            self.data[matched_key] = dict(doc)
            return MagicMock(modified_count=1, matched_count=1)
            
        if upsert:
            key = doc.get("_id")
            if not key:
                if self.name == "jobs" and doc.get("job_id"):
                    key = doc.get("job_id")
                elif self.name == "assets" and doc.get("checksum"):
                    key = doc.get("checksum")
                else:
                    key = query.get("_id")
                    if not key and self.name == "jobs" and query.get("job_id"):
                        key = query.get("job_id")
                    elif not key and self.name == "assets" and query.get("checksum"):
                        key = query.get("checksum")
            if not key:
                import uuid
                key = str(uuid.uuid4())
            doc_copy = dict(doc)
            if "_id" not in doc_copy:
                doc_copy["_id"] = key
            self.data[key] = doc_copy
            return MagicMock(modified_count=1, matched_count=0, upserted_id=key)
            
        return MagicMock(modified_count=0, matched_count=0)


class MockDatabase:
    def __init__(self):
        self._collections = {}

    def __getattr__(self, name):
        if name not in self._collections:
            self._collections[name] = MockCollection(name=name)
        return self._collections[name]

    def __getitem__(self, name):
        return self.__getattr__(name)


class MockAdmin:
    def command(self, cmd_name, *args, **kwargs):
        if cmd_name == 'ping':
            return {"ok": 1.0}
        return {}


class MockSession:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    def start_transaction(self):
        pass
    def commit_transaction(self):
        pass
    def abort_transaction(self):
        pass


class MockMongoClient:
    def __init__(self):
        self._db = MockDatabase()
        self.admin = MockAdmin()
        
    def get_database(self, name=None):
        return self._db

    def start_session(self, **kwargs):
        return MockSession()



@pytest.fixture
def dummy_package(tmp_path):
    """Creates a temporary zip package representing a dummy course ingestion payload."""
    package_dir = tmp_path / "dummy_src"
    package_dir.mkdir()
    
    # 1. dummy manifest
    manifest = {
        "title": "Dummy Ingestion Course",
        "identifier": "dummy_course_999",
        "lessons": ["dummy/lesson1.html"],
        "quiz": "dummy/quiz.json"
    }
    manifest_dir = package_dir / "dummy"
    manifest_dir.mkdir()
    
    with open(manifest_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    # 2. dummy lesson body
    with open(manifest_dir / "lesson1.html", "w", encoding="utf-8") as f:
        f.write("<h1>Introduction to Ingestion</h1><p>Deterministic dummy lesson.</p>")

    # 3. dummy quiz questions
    quiz = {
        "title": "Ingestion Quiz",
        "description": "Verify the ingestion works.",
        "questions": [
            {
                "text": "Does this work?",
                "points": 10.0,
                "answers": [
                    {"text": "Yes", "correct": True},
                    {"text": "No", "correct": False}
                ]
            }
        ]
    }
    with open(manifest_dir / "quiz.json", "w", encoding="utf-8") as f:
        json.dump(quiz, f)

    # Zip it up
    zip_path = tmp_path / "dummy_package.zip"
    with zipfile.ZipFile(zip_path, "w") as zip_ref:
        for p in package_dir.glob("**/*"):
            if p.is_file():
                zip_ref.write(p, p.relative_to(package_dir))

    # Calculate SHA-256 for testing
    import hashlib
    sha = hashlib.sha256()
    with open(zip_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
            
    return zip_path, sha.hexdigest()


def test_end_to_end_dummy_ingestion(dummy_package, tmp_path):
    zip_path, checksum = dummy_package
    
    # Setup mocks
    db_client = MockMongoClient()
    
    # Mock S3 Client
    s3_mock = MagicMock()
    # Mock download_file to copy the file to local path
    def mock_download(Bucket, Key, Filename, ExtraArgs=None):
        import shutil
        shutil.copy(zip_path, Filename)
    s3_mock.download_file.side_effect = mock_download
    s3_mock.head_object.return_value = {
        "Metadata": {"sha256": checksum},
        "ETag": f'"{checksum}"'
    }

    # Mock SQS Client
    sqs_mock = MagicMock()
    sqs_message = {
        "Messages": [
            {
                "MessageId": "msg_1234",
                "ReceiptHandle": "receipt_1234",
                "Body": json.dumps({
                    "job_id": "job_dummy_run_1",
                    "bucket": "mock-bucket",
                    "key": "dummy_package.zip",
                    "version_id": "v1"
                }),
                "Attributes": {
                    "ApproximateReceiveCount": "1"
                }
            }
        ]
    }
    sqs_mock.receive_message.return_value = sqs_message

    # Setup registries
    provider_registry = ProviderRegistry()
    provider_registry.register(DummyProvider())
    normalizer = CanonicalNormalizer()

    # Create Ingestion Queue Listener
    listener = IngestionQueueListener(
        queue_url="https://sqs.mock/queue",
        db_client=db_client,
        provider_registry=provider_registry,
        normalizer=normalizer,
        s3_client=s3_mock,
        sqs_client=sqs_mock,
        visibility_timeout_secs=60,
        temp_base_dir=tmp_path
    )

    # Poll and process
    listener.poll_messages(max_messages=1, wait_time_seconds=1)

    # --- ASSERTIONS ---

    # 1. Assert SQS deleted the message
    sqs_mock.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.mock/queue",
        ReceiptHandle="receipt_1234"
    )

    # 2. Assert Job Document status is SUCCESS in database
    db = db_client.get_database()
    job = db.jobs.find_one({"job_id": "job_dummy_run_1"})
    assert job is not None
    assert job["status"] == JobState.SUCCESS.value
    assert job["duration_seconds"] >= 0.0

    # 3. Assert append-only events are chronological and rich
    events = job["events"]
    assert len(events) >= 5
    assert events[0]["stage"] == "CREATED"
    assert events[1]["stage"] == "DOWNLOAD_STARTED"
    # Success event at the end
    assert events[-1]["stage"] == "SUCCESS"

    # 4. Assert disk-based reference files exist and are loadable
    assert job["provider_model_ref"] is not None
    assert job["canonical_course_ref"] is not None
    assert job["normalized_canonical_course_ref"] is not None

    # Load context back to assert deserialization and stable fingerprint
    from ucae.workflow.context import PipelineContext
    context = PipelineContext(job_id="job_dummy_run_1")
    # Find the extracted folder inside tmp_path dynamically
    extracted_dirs = list(tmp_path.glob("**/extracted"))
    assert len(extracted_dirs) > 0
    workspace_root = extracted_dirs[0]

    context.workspace = ExtractedWorkspace(
        root_path=workspace_root,
        is_temporary=False
    )
    context.provider_model_ref = job["provider_model_ref"]
    context.canonical_course_ref = job["canonical_course_ref"]
    context.normalized_canonical_course_ref = job["normalized_canonical_course_ref"]

    # Deserialization check
    provider_model = context.load_provider_model()
    assert provider_model["title"] == "Dummy Ingestion Course"
    
    canonical_course = context.load_canonical_course(is_normalized=False)
    assert canonical_course.title == "Dummy Ingestion Course"
    assert len(canonical_course.modules) == 1
    assert canonical_course.modules[0].title == "Module 1"

    # Fingerprint check
    normalized_course = context.load_canonical_course(is_normalized=True)
    fp = normalizer.compute_content_fingerprint(normalized_course)
    
    # Assert fingerprint matches
    assert fp == normalizer.compute_content_fingerprint(normalized_course)


def test_duplicate_sqs_event_handling(dummy_package, tmp_path):
    zip_path, checksum = dummy_package
    db_client = MockMongoClient()
    
    # Pre-populate SUCCESS job in MongoDB
    db = db_client.get_database()
    db.jobs.data["job_duplicate_1"] = {
        "job_id": "job_duplicate_1",
        "status": JobState.SUCCESS.value,
        "events": []
    }
    
    s3_mock = MagicMock()
    sqs_mock = MagicMock()
    sqs_message = {
        "Messages": [
            {
                "MessageId": "msg_dup_123",
                "ReceiptHandle": "receipt_dup_123",
                "Body": json.dumps({
                    "job_id": "job_duplicate_1",
                    "bucket": "mock-bucket",
                    "key": "dummy_package.zip",
                    "version_id": "v1"
                }),
                "Attributes": {"ApproximateReceiveCount": "1"}
            }
        ]
    }
    sqs_mock.receive_message.return_value = sqs_message

    provider_registry = ProviderRegistry()
    provider_registry.register(DummyProvider())
    normalizer = CanonicalNormalizer()

    listener = IngestionQueueListener(
        queue_url="https://sqs.mock/queue",
        db_client=db_client,
        provider_registry=provider_registry,
        normalizer=normalizer,
        s3_client=s3_mock,
        sqs_client=sqs_mock,
        visibility_timeout_secs=60,
        temp_base_dir=tmp_path
    )

    listener.poll_messages(max_messages=1, wait_time_seconds=1)

    # 1. Verify message was deleted from SQS (acked/skipped)
    sqs_mock.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.mock/queue",
        ReceiptHandle="receipt_dup_123"
    )
    # 2. Verify download was never started (skipped processing)
    s3_mock.download_file.assert_not_called()


def test_corrupted_zip_quarantine(tmp_path):
    # Create corrupted zip file
    bad_zip_path = tmp_path / "corrupted_package.zip"
    with open(bad_zip_path, "w") as f:
        f.write("This is not a zip archive, it is text.")

    db_client = MockMongoClient()
    s3_mock = MagicMock()
    # Mock download to write the corrupted archive
    def mock_download(Bucket, Key, Filename, ExtraArgs=None):
        import shutil
        shutil.copy(bad_zip_path, Filename)
    s3_mock.download_file.side_effect = mock_download
    s3_mock.head_object.return_value = {
        "Metadata": {"sha256": "bad-hash"},
        "ETag": '"bad-hash"'
    }

    sqs_mock = MagicMock()
    sqs_message = {
        "Messages": [
            {
                "MessageId": "msg_corrupt_123",
                "ReceiptHandle": "receipt_corrupt_123",
                "Body": json.dumps({
                    "job_id": "job_corrupt_1",
                    "bucket": "mock-bucket",
                    "key": "corrupted_package.zip",
                    "version_id": "v1"
                }),
                "Attributes": {"ApproximateReceiveCount": "1"}
            }
        ]
    }
    sqs_mock.receive_message.return_value = sqs_message

    provider_registry = ProviderRegistry()
    provider_registry.register(DummyProvider())
    normalizer = CanonicalNormalizer()

    listener = IngestionQueueListener(
        queue_url="https://sqs.mock/queue",
        db_client=db_client,
        provider_registry=provider_registry,
        normalizer=normalizer,
        s3_client=s3_mock,
        sqs_client=sqs_mock,
        visibility_timeout_secs=60,
        temp_base_dir=tmp_path
    )

    listener.poll_messages(max_messages=1, wait_time_seconds=1)

    # 1. Assert SQS deleted the message (as it is a non-retryable QuarantineError)
    sqs_mock.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.mock/queue",
        ReceiptHandle="receipt_corrupt_123"
    )
    # 2. Assert status is set to QUARANTINED in database
    db = db_client.get_database()
    job = db.jobs.find_one({"job_id": "job_corrupt_1"})
    assert job is not None
    assert job["status"] == JobState.QUARANTINED.value
    assert "Checksum validation failed" in job["error_message"] or "zipfile.BadZipFile" in job["error_message"] or "Bad zip file" in job["error_message"]


def test_mongo_outage_handling(dummy_package, tmp_path):
    zip_path, checksum = dummy_package
    
    # Mock DB that raises errors to simulate MongoDB network outage
    class OutageDatabase:
        def get_database(self):
            raise Exception("MongoDB Connection Refused")
    db_client = OutageDatabase()

    s3_mock = MagicMock()
    sqs_mock = MagicMock()
    sqs_message = {
        "Messages": [
            {
                "MessageId": "msg_outage_123",
                "ReceiptHandle": "receipt_outage_123",
                "Body": json.dumps({
                    "job_id": "job_outage_1",
                    "bucket": "mock-bucket",
                    "key": "dummy_package.zip",
                    "version_id": "v1"
                }),
                "Attributes": {"ApproximateReceiveCount": "1"}
            }
        ]
    }
    sqs_mock.receive_message.return_value = sqs_message

    provider_registry = ProviderRegistry()
    provider_registry.register(DummyProvider())
    normalizer = CanonicalNormalizer()

    listener = IngestionQueueListener(
        queue_url="https://sqs.mock/queue",
        db_client=db_client,
        provider_registry=provider_registry,
        normalizer=normalizer,
        s3_client=s3_mock,
        sqs_client=sqs_mock,
        visibility_timeout_secs=60,
        temp_base_dir=tmp_path
    )

    listener.poll_messages(max_messages=1, wait_time_seconds=1)

    # 1. Assert message was NOT deleted (remained in queue for retry)
    sqs_mock.delete_message.assert_not_called()
    # 2. Assert visibility timeout was shortened to 10s to trigger quick retry or DLQ routing
    sqs_mock.change_message_visibility.assert_called_once_with(
        QueueUrl="https://sqs.mock/queue",
        ReceiptHandle="receipt_outage_123",
        VisibilityTimeout=10
    )


def test_partial_asset_upload_recovery(tmp_path):
    from ucae.canonical.assets import AssetRegistry
    db_client = MockMongoClient()
    registry = AssetRegistry(db_client)

    checksum = "dummy-checksum-123"
    worker_1 = "worker_1"
    worker_2 = "worker_2"

    # Worker 1 starts upload but crashes (status is UPLOADING)
    status, asset = registry.reserve_asset(checksum, worker_id=worker_1, lease_secs=2)
    assert status == "RESERVED"
    registry.start_upload(checksum, worker_id=worker_1, lease_secs=2)

    # Wait for lease to expire
    import time
    time.sleep(2.1)

    # Worker 2 tries to reserve it
    status, asset = registry.reserve_asset(checksum, worker_id=worker_2, lease_secs=300)
    assert status == "RESERVED" # Recovered!

    # Worker 2 completes upload
    registry.start_upload(checksum, worker_id=worker_2, lease_secs=300)
    registry.start_verification(checksum, worker_id=worker_2, lease_secs=60)
    registry.complete_upload(
        checksum=checksum,
        s3_key="prefix/asset.pdf",
        cdn_url="https://cdn/prefix/asset.pdf",
        size_bytes=1000,
        mime_type="application/pdf"
    )

    # Verify asset is completed
    asset = registry.get_asset(checksum)
    assert asset is not None
    assert asset.cdn_url == "https://cdn/prefix/asset.pdf"


def test_idempotent_approval_endpoint(monkeypatch):
    import os
    monkeypatch.setenv("ULCP_MONGODB_URI", "mongodb://mock:27017")
    monkeypatch.setenv("PLATFORM_MONGODB_URI", "mongodb://mock:27017")
    import json
    import unittest.mock as mock
    from fastapi.testclient import TestClient
    from api.main import app
    from core.job_state_machine import JobState
    from services.canonical_migration_service import get_migration_service
    from api.middleware_enhanced import require_api_key

    # Bypass API key validation in test
    app.dependency_overrides[require_api_key] = lambda: "test-key"

    service = get_migration_service()
    
    # Ensure job is registered as COMPLETED
    job_id = "job_approve_123"
    job = service.orchestrator.create_job(
        job_id=job_id,
        source_type="zip",
        source_path="mock.zip"
    )
    job.course_id = "course_approve_123"
    job.source_metadata["university_id"] = "univ_approve_123"
    job.checkpoint(JobState.COMPLETED, 100, "Complete")
    service.orchestrator._persist_job(job)
    
    client = TestClient(app)
    
    # Mock SQS
    with mock.patch("boto3.client") as mock_boto:
        mock_sqs = mock.MagicMock()
        mock_boto.return_value = mock_sqs
        mock_sqs.send_message.return_value = {"MessageId": "msg_promo_123"}
        
        # Mock env variable
        monkeypatch.setenv("PROMOTION_FIFO_QUEUE_URL", "https://sqs.mock/fifo-queue.fifo")
        
        # Approve
        response = client.post(f"/api/v1/jobs/{job_id}/approve", headers={"X-API-Key": "test-key"})
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "approved"
        assert data["message_id"] == "msg_promo_123"
        
        # Verify SQS FIFO parameters
        mock_sqs.send_message.assert_called_once()
        call_kwargs = mock_sqs.send_message.call_args[1]
        
        from core.idempotency import build_promotion_dedup_id
        expected_dedup_id = build_promotion_dedup_id("course_approve_123", "fp_course_approve_123_job_approve_123")
        assert call_kwargs["MessageGroupId"] == "univ_approve_123"
        assert call_kwargs["MessageDeduplicationId"] == expected_dedup_id


def test_promotion_worker_flow():
    from ucae.worker.promotion_listener import PromotionQueueListener
    
    ulcp_client = MockMongoClient()
    platform_client = MockMongoClient()
    
    ulcp_db = ulcp_client.get_database()
    platform_db = platform_client.get_database()
    
    job_id = "job_promo_99"
    course_id = "course_promo_99"
    fingerprint = "fp_promo_99"
    
    ulcp_db.jobs.data[job_id] = {
        "job_id": job_id,
        "course_id": course_id,
        "content_fingerprint": fingerprint,
        "status": "completed"
    }
    
    ulcp_db.courses.data[course_id] = {
        "_id": course_id,
        "slug": course_id,
        "title": "Promotion Course Test",
        "content_fingerprint": fingerprint
    }
    
    sqs_mock = MagicMock()
    
    listener = PromotionQueueListener(
        queue_url="https://sqs.mock/promotion-fifo-queue.fifo",
        ulcp_db_client=ulcp_client,
        platform_db_client=platform_client,
        sqs_client=sqs_mock,
        worker_id="test_worker"
    )
    
    message = {
        "MessageId": "msg_promo_99",
        "ReceiptHandle": "receipt_promo_99",
        "Body": json.dumps({
            "job_id": job_id,
            "course_id": course_id,
            "correlation_id": "corr_promo_99"
        })
    }
    
    listener._process_message(message, receive_count=1)
    
    promoted_course = platform_db.courses.find_one({"slug": course_id})
    assert promoted_course is not None
    assert promoted_course["title"] == "Promotion Course Test"
    
    from core.idempotency import build_promotion_dedup_id
    expected_dedup_id = build_promotion_dedup_id(course_id, fingerprint)
    execution = platform_db.promotion_executions.find_one({"_id": expected_dedup_id})
    assert execution is not None
    assert execution["job_id"] == job_id
    assert execution["status"] == "COMPLETED"
    
    event = platform_db.promotion_events.find_one({"fingerprint": fingerprint, "stage": "PROMOTED"})
    assert event is not None
    assert event["stage"] == "PROMOTED"
    
    # Test replay (idempotency check)
    del platform_db.courses.data[course_id]
    
    listener._process_message(message, receive_count=2)
    assert platform_db.courses.find_one({"slug": course_id}) is None
    
    skipped_event = platform_db.promotion_events.find_one({"stage": "SKIPPED"})
    assert skipped_event is not None
    assert "skipped" in skipped_event["message"].lower()

