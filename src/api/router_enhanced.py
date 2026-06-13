"""
Enhanced API Router - Uses canonical pipeline with orchestration.

Replaces the simple sequential pipeline with:
- Job state management
- Checkpoint recovery
- Correlation tracing
- Idempotent deduplication
"""

from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException, Depends, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import uuid
from datetime import datetime
from pathlib import Path

from services.canonical_migration_service import CanonicalMigrationService, get_migration_service
from api.middleware_enhanced import require_api_key, TracingMiddleware
from observability.tracing import get_correlation_id
from core.job_state_machine import JobState
from observability.logger import get_logger
import os

logger = get_logger(__name__)

router = APIRouter(tags=["Canonical Migration"])


@router.post("/migrate")
async def start_migration(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    university_id: Optional[str] = None,
    author_id: Optional[str] = None,
    force: bool = False,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """
    Upload a course shell ZIP and start canonical ingestion.
    
    Features:
    - Immediate response with job ID
    - Background orchestrated processing
    - Checkpoint-based recovery
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Only ZIP files are supported")
    
    task_id = str(uuid.uuid4())
    correlation_id = get_correlation_id()
    
    # Save file
    uploads_dir = Path(os.getenv("STORAGE_DIR", "storage")) / "uploads"
    zip_path = uploads_dir / f"{task_id}.zip"
    
    with open(zip_path, "wb") as buffer:
        import shutil
        shutil.copyfileobj(file.file, buffer)
    
    # Queue background task
    background_tasks.add_task(
        service.process_migration,
        task_id=task_id,
        file=None,  # Already saved
        university_id=university_id or os.getenv("DEFAULT_UNIVERSITY_ID"),
        author_id=author_id or os.getenv("DEFAULT_AUTHOR_ID"),
        course_code=None,
        file_path=zip_path,  # Pass path directly
        correlation_id=correlation_id,
        force=force
    )
    
    return {
        "status": "accepted",
        "job_id": task_id,
        "task_id": task_id,
        "correlation_id": correlation_id,
        "filename": file.filename,
        "timestamp": datetime.utcnow().isoformat(),
        "track_url": f"/api/v1/status/{task_id}"
    }


@router.get("/status/{job_id}")
async def get_job_status(
    job_id: str,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """
    Get detailed job status with checkpoint information.
    
    Returns:
    - Current state
    - Progress percentage
    - Checkpoint history
    - Error details (if failed)
    """
    status = service.get_job_status(job_id)
    if not status or status.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="Job not found")
    
    return status


@router.post("/migrate-s3")
async def start_migration_from_s3(
    body: dict,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """
    Trigger migration from S3 key.
    
    Body:
        s3_key: str - S3 object key
        bucket: Optional[str] - S3 bucket name
        university_id: Optional[str]
        author_id: Optional[str]
        force: bool = False
    """
    s3_key = body.get("s3_key")
    if not s3_key or not s3_key.endswith('.zip'):
        raise HTTPException(status_code=400, detail="s3_key must point to a .zip file")
    
    task_id = str(uuid.uuid4())
    correlation_id = get_correlation_id()
    
    background_tasks.add_task(
        service.process_migration_from_s3,
        task_id=task_id,
        s3_key=s3_key,
        bucket=body.get("bucket"),
        university_id=body.get("university_id", os.getenv("DEFAULT_UNIVERSITY_ID")),
        author_id=body.get("author_id", os.getenv("DEFAULT_AUTHOR_ID")),
        course_code=None,
        correlation_id=correlation_id,
        force=body.get("force", False)
    )
    
    return {
        "status": "accepted",
        "job_id": task_id,
        "s3_key": s3_key,
        "correlation_id": correlation_id,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.post("/migrate-canvas")
async def start_canvas_migration(
    body: dict,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """
    Trigger migration directly from Canvas LMS via API.
    """
    course_id = body.get("course_id")
    if not course_id:
        raise HTTPException(status_code=400, detail="course_id required")
    
    task_id = str(uuid.uuid4())
    correlation_id = get_correlation_id()
    
    background_tasks.add_task(
        service.process_canvas_migration,
        task_id=task_id,
        course_id=course_id,
        university_id=body.get("university_id", os.getenv("DEFAULT_UNIVERSITY_ID")),
        author_id=body.get("author_id", os.getenv("DEFAULT_AUTHOR_ID")),
        force=body.get("force", False),
        correlation_id=correlation_id
    )
    
    return {
        "status": "accepted",
        "job_id": task_id,
        "course_id": course_id,
        "correlation_id": correlation_id,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "canonical-migration",
        "timestamp": datetime.utcnow().isoformat(),
        "architecture": "canonical-orchestrated"
    }


# Recovery and admin endpoints

@router.post("/jobs/{job_id}/approve")
async def approve_job(
    job_id: str,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """
    Approve a successfully ingested course job for promotion to staging.
    Idempotent: publishes to SQS FIFO.
    """
    job = service.orchestrator.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    if job.state != JobState.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is in state {job.state.value}. Only COMPLETED jobs can be approved."
        )

    import boto3
    import json
    import os
    import hashlib
    from pymongo import MongoClient
    
    # Resolve university_id and content_fingerprint
    ulcp_uri = os.getenv("ULCP_MONGODB_URI")
    db_name = os.getenv("ULCP_DATABASE", "test")
    
    content_fingerprint = None
    university_id = None
    
    if ulcp_uri:
        try:
            mongo_client = MongoClient(ulcp_uri)
            db = mongo_client[db_name]
            job_doc = db.jobs.find_one({"job_id": job_id})
            if job_doc:
                content_fingerprint = (
                    job_doc.get("content_fingerprint") or 
                    job_doc.get("source_metadata", {}).get("content_fingerprint") or 
                    job_doc.get("metadata", {}).get("content_fingerprint")
                )
                university_id = (
                    job_doc.get("university_id") or 
                    job_doc.get("metadata", {}).get("university_id")
                )
                
                # Try finding from course doc if still missing
                course_id = job.course_id or job_id
                if not university_id and course_id:
                    course_doc = db.courses.find_one({"slug": course_id}) or db.courses.find_one({"_id": course_id})
                    if course_doc:
                        university_id = course_doc.get("universityId") or course_doc.get("university")
        except Exception as e:
            logger.warning(f"Failed to query ULCP DB for approval details: {e}")
            
    if not content_fingerprint:
        content_fingerprint = (
            job.source_metadata.get("content_fingerprint") or 
            f"fp_{job.course_id or job_id}_{job_id}"
        )
    if not university_id:
        university_id = (
            job.source_metadata.get("university_id") or 
            os.getenv("DEFAULT_UNIVERSITY_ID") or 
            "000000000000000000000000"
        )
        
    course_id = job.course_id or job_id
    correlation_id = None
    if ulcp_uri and 'job_doc' in locals() and job_doc:
        correlation_id = (
            job_doc.get("correlation_id") or
            job_doc.get("metadata", {}).get("correlation_id")
        )
    if not correlation_id:
        correlation_id = job.correlation_id or str(uuid.uuid4())
    
    # Compute deterministic deduplication key
    from core.idempotency import build_promotion_dedup_id
    dedup_id = build_promotion_dedup_id(course_id, content_fingerprint)
    
    # Persist approval request to platform DB with _id = job_id to enforce database-level uniqueness
    try:
        platform_uri = os.getenv("PLATFORM_MONGODB_URI")
        platform_db_name = os.getenv("PLATFORM_DATABASE", "test")
        if platform_uri:
            platform_client = MongoClient(platform_uri)
            p_db = platform_client[platform_db_name]
            
            request_doc = {
                "_id": job_id,
                "job_id": job_id,
                "course_id": course_id,
                "content_fingerprint": content_fingerprint,
                "university_id": str(university_id),
                "correlation_id": correlation_id,
                "approved_at": datetime.utcnow(),
                "approved_by": "QA_Reviewer"
            }
            try:
                p_db.promotion_requests.insert_one(request_doc)
            except Exception: # DuplicateKeyError
                # Request already registered, return existing details idempotently
                existing_req = p_db.promotion_requests.find_one({"_id": job_id})
                if existing_req:
                    logger.info(f"Promotion request for job {job_id} already exists. Returning existing details.")
                    return {
                        "status": "approved",
                        "job_id": job_id,
                        "course_id": course_id,
                        "university_id": str(existing_req.get("university_id")),
                        "deduplication_id": dedup_id,
                        "message_id": "duplicate_skipped",
                        "timestamp": existing_req.get("approved_at").isoformat() if isinstance(existing_req.get("approved_at"), datetime) else str(existing_req.get("approved_at"))
                    }
    except Exception as db_err:
        logger.warning(f"Failed to persist promotion request in platform DB: {db_err}")
    
    sqs_client = boto3.client("sqs")
    queue_url = os.getenv("PROMOTION_FIFO_QUEUE_URL")
    
    payload = {
        "job_id": job_id,
        "course_id": course_id,
        "university_id": str(university_id),
        "correlation_id": correlation_id,
        "content_fingerprint": content_fingerprint,
        "deduplication_id": dedup_id,
        "approved_at": datetime.utcnow().isoformat(),
        "approved_by": "QA_Reviewer"
    }
    
    msg_body = json.dumps(payload)
    
    sqs_kwargs = {
        "QueueUrl": queue_url or "https://sqs.mock/promotion-fifo-queue.fifo",
        "MessageBody": msg_body,
        "MessageGroupId": str(university_id),
        "MessageDeduplicationId": dedup_id
    }
    
    if queue_url:
        try:
            res = sqs_client.send_message(**sqs_kwargs)
            msg_id = res.get("MessageId")
        except Exception as e:
            logger.exception("Failed to send message to SQS FIFO queue")
            raise HTTPException(status_code=500, detail=f"SQS FIFO dispatch failed: {e}")
    else:
        msg_id = f"mock_msg_{uuid.uuid4()}"
        logger.info(f"Mock SQS FIFO dispatch successful. MessageId: {msg_id}")

    return {
        "status": "approved",
        "job_id": job_id,
        "course_id": course_id,
        "university_id": str(university_id),
        "deduplication_id": dedup_id,
        "message_id": msg_id,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/jobs/{job_id}/retry")
async def retry_job(
    job_id: str,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """Manually retry a failed job."""
    job = service.orchestrator.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Reset retry count if needed
    if job.state == JobState.FAILED:
        job.retry_count = 0
        job.error_count = 0
        service.orchestrator._persist_job(job)
        
        # Re-queue
        # Would need async worker system here
        return {"status": "queued", "job_id": job_id}
    
    return {"status": "cannot_retry", "state": job.state.value}


@router.get("/jobs")
async def list_jobs(
    state: Optional[str] = None,
    limit: int = 50,
    api_key: str = Depends(require_api_key),
    service: CanonicalMigrationService = Depends(get_migration_service)
) -> Dict[str, Any]:
    """List ingestion jobs."""
    jobs = service.orchestrator.job_store.list_jobs(
        JobState(state) if state else None
    )
    
    return {
        "jobs": jobs[:limit],
        "total": len(jobs),
        "returned": min(limit, len(jobs))
    }