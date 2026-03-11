from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from typing import Dict, Any
import uuid
from datetime import datetime
from pathlib import Path

from .service import MigrationService

router = APIRouter(tags=["Migration"])
migration_service = MigrationService()

@router.post("/migrate")
async def start_migration(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
) -> Dict[str, Any]:
    """
    Upload a course shell ZIP and start the migration process.
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Only ZIP files are supported")
    
    # Generate a unique task ID
    task_id = str(uuid.uuid4())
    
    # Save the file and start processing in background
    background_tasks.add_task(
        migration_service.process_migration, 
        task_id, 
        file
    )
    
    return {
        "status": "accepted",
        "task_id": task_id,
        "filename": file.filename,
        "timestamp": datetime.utcnow().isoformat()
    }

@router.get("/status/{task_id}")
async def get_status(task_id: str) -> Dict[str, Any]:
    """
    Get the current status of a migration task.
    """
    status = migration_service.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return status

@router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
