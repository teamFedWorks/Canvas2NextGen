from .celery_app import app
from ..core.pipeline import MigrationPipeline
from ..observability.logger import get_logger
from pathlib import Path
import os

logger = get_logger(__name__)

@app.task(bind=True, name="ingest_course")
def ingest_course_task(self, 
                       course_path: str, 
                       university_id: str, 
                       author_id: str, 
                       course_code: str, 
                       task_id: str):
    """
    Celery task to run the full migration pipeline.
    """
    logger.info(f"Worker received task: {task_id}")
    
    pipeline = MigrationPipeline(
        course_directory=Path(course_path),
        university_id=university_id,
        author_id=author_id,
        course_code=course_code,
        task_id=task_id,
        on_progress=lambda stage, prog, msg: self.update_state(
            state="PROGRESS",
            meta={"stage": stage, "progress": prog, "message": msg}
        )
    )
    
    report = pipeline.run()
    
    if report.status.value == "failure":
        raise Exception(f"Migration failed for {task_id}. Check logs in MongoDB.")
        
    return {
        "status": "success",
        "task_id": task_id,
        "content_counts": report.migrated_content_counts
    }
