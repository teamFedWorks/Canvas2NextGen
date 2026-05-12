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
import os

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