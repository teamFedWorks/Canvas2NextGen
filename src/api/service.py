"""
Migration Service - Orchestrates migration tasks.

Handles background processing of course migrations from various sources.
Now uses MongoDB for persistent job state and a cleaner pipeline integration.
"""

import os
import shutil
import uuid
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import UploadFile

from core.pipeline import MigrationPipeline
from exporters.mongodb_exporter import MongoDBExporter
from worker.ingestion_worker import IngestionWorker
from observability.logger import get_logger
from utils.zip_utils import safe_extractall

logger = get_logger(__name__)


class MigrationService:
    """
    Service for managing migration tasks with MongoDB-backed state.
    """
    
    def __init__(self):
        self.storage_dir = Path(os.getenv("STORAGE_DIR", "storage"))
        self.uploads_dir = self.storage_dir / "uploads"
        self.outputs_dir = self.storage_dir / "outputs"
        self.db_uploader = MongoDBExporter()
        
        # Ensure directories exist
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Retrieve task status from MongoDB."""
        job = self.db_uploader.get_job(task_id)
        if not job:
            return {"task_id": task_id, "status": "not_found", "message": "Task ID not found in database"}
        
        # Standardize response format for the frontend
        return {
            "task_id": task_id,
            "status": job.get("status"),
            "progress": job.get("progress", 0),
            "started_at": job.get("startedAt"),
            "completed_at": job.get("completedAt"),
            "message": job.get("logs")[-1] if job.get("logs") else "Processing...",
            "logs": job.get("logs", [])
        }

    async def process_migration(
        self, 
        task_id: str, 
        file: UploadFile,
        university_id: Optional[str] = None,
        author_id: Optional[str] = None,
        course_code: Optional[str] = None
    ):
        """
        Background task to process a migration from an uploaded ZIP file.
        """
        extract_dir = Path(tempfile.mkdtemp(prefix=f"migration_{task_id}_"))
        zip_path = self.uploads_dir / f"{task_id}.zip"
        
        try:
            logger.info("Initializing migration job", extra={"task_id": task_id})
            self.db_uploader.create_job(task_id, s3_key=None)
            
            # Step 1: Save uploaded file
            self._update_progress(task_id, "processing", "Saving uploaded file...", 2)
            with open(zip_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            # Step 2: Extraction
            self._update_progress(task_id, "processing", "Extracting package...", 5)
            import zipfile
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                safe_extractall(zip_ref, extract_dir)
            
            # Step 3: Run Pipeline
            output_dir = self.outputs_dir / task_id
            pipeline = MigrationPipeline(
                course_directory=extract_dir,
                university_id=university_id,
                author_id=author_id,
                output_directory=output_dir,
                on_progress=self._get_progress_callback(task_id),
                course_code=course_code,
                task_id=task_id
            )
            
            # The pipeline now handles transform -> asset upload -> DB write
            report = pipeline.run()
            
            if report and report.status.value == "success":
                self._update_progress(task_id, "completed", "Migration successful", 100)
                logger.info("Migration job completed successfully", extra={"task_id": task_id})
            else:
                self._update_progress(task_id, "failed", "Pipeline execution failed", 100)
                logger.error("Migration pipeline failed", extra={"task_id": task_id})

        except Exception as e:
            logger.error("Critical failure during migration", extra={"task_id": task_id, "error": str(e)})
            self._update_progress(task_id, "failed", f"Critical error: {str(e)}", 100)
        
        finally:
            # Cleanup
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            if zip_path.exists():
                zip_path.unlink()

    async def process_migration_from_s3(
        self, 
        task_id: str, 
        s3_key: str, 
        bucket: Optional[str] = None,
        university_id: Optional[str] = None,
        author_id: Optional[str] = None,
        course_code: Optional[str] = None
    ):
        """
        Background task to download from S3 and migrate.
        """
        from utils.s3_utils import S3Downloader
        
        self.db_uploader.create_job(task_id, s3_key=s3_key)
        zip_path = self.uploads_dir / f"{task_id}.zip"
        extract_dir = Path(tempfile.mkdtemp(prefix=f"migration_s3_{task_id}_"))
        
        try:
            self._update_progress(task_id, "processing", f"Downloading from S3: {s3_key}", 5)
            
            downloader = S3Downloader(bucket=bucket)
            if downloader.download(s3_key, zip_path):
                # Extraction
                self._update_progress(task_id, "processing", "Extracting package...", 10)
                import zipfile
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    safe_extractall(zip_ref, extract_dir)

                # Pipeline
                output_dir = self.outputs_dir / task_id
                pipeline = MigrationPipeline(
                    course_directory=extract_dir,
                    university_id=university_id,
                    author_id=author_id,
                    output_directory=output_dir,
                    on_progress=self._get_progress_callback(task_id),
                    course_code=course_code,
                    task_id=task_id
                )
                report = pipeline.run()
                
                if report and report.status.value == "success":
                    self._update_progress(task_id, "completed", "Migration successful", 100)
                else:
                    self._update_progress(task_id, "failed", "Pipeline execution failed", 100)
            else:
                self._update_progress(task_id, "failed", "S3 download failed", 100)
                
        except Exception as e:
            logger.error("S3 migration failed", extra={"task_id": task_id, "error": str(e)})
            self._update_progress(task_id, "failed", str(e), 100)
        finally:
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            if zip_path.exists():
                zip_path.unlink()

    async def process_hierarchical_migration(self, task_id: str, course_id: str):
        """
        Resolve S3 path via metadata and migrate.
        """
        from utils.dynamodb_utils import MetadataProvider
        from utils.s3_utils import S3Downloader
        
        self.db_uploader.create_job(task_id)
        
        try:
            self._update_progress(task_id, "processing", "Resolving course metadata...", 5)
            
            meta_provider = MetadataProvider()
            meta = meta_provider.get_course_metadata(course_id)
            if not meta:
                raise ValueError(f"Metadata not found for course {course_id}")
            
            university_id = meta.get('university_id')
            program_id = meta.get('program_id')
            course_code = meta.get('course_code')
            author_id = meta.get('author_id')  # Might be None

            if not all([university_id, program_id, course_code]):
                 raise ValueError(f"Incomplete metadata for course {course_id}")

            # Construct S3 key
            downloader = S3Downloader()
            s3_key = downloader.construct_hierarchical_key(university_id, program_id, course_code)
            
            logger.info("Resolved hierarchical S3 key", extra={"task_id": task_id, "s3_key": s3_key})
            
            # Delegate to S3 processor with metadata
            await self.process_migration_from_s3(
                task_id, 
                s3_key, 
                university_id=university_id,
                author_id=author_id,
                course_code=course_code
            )
        except Exception as e:
            logger.error("Hierarchical migration failed", extra={"task_id": task_id, "error": str(e)})
            self._update_progress(task_id, "failed", str(e), 100)

    def _update_progress(self, task_id: str, status: str, message: str, progress: int):
        """Helper to update DB and log consistently."""
        self.db_uploader.update_job_status(task_id, status, log_msg=message, progress=progress)
        logger.debug(f"Task {task_id} update: {message} ({progress}%)")

    def _get_progress_callback(self, task_id: str):
        """Returns a callback function for the MigrationPipeline."""
        def callback(stage: str, progress: int, message: str):
            # Scale pipeline progress (0-100) into overall job progress (10-95)
            overall_pct = 10 + int(progress * 0.85)
            self._update_progress(task_id, "processing", f"[{stage}] {message}", overall_pct)
        return callback

    async def process_canvas_migration(
        self, 
        task_id: str, 
        course_id: str, 
        university_id: Optional[str] = None, 
        author_id: Optional[str] = None,
        force: bool = False
    ):
        """
        Background task to extract from Canvas API and migrate.
        """
        s3_bucket = os.getenv("S3_CDN_BUCKET")
        cdn_url = os.getenv("CDN_URL")
        
        worker = IngestionWorker(s3_bucket=s3_bucket, cdn_url=cdn_url)
        
        try:
            self._update_progress(task_id, "processing", f"Initializing Canvas API extraction: {course_id}", 5)
            
            # Create the job in DB first
            self.db_uploader.create_job(task_id, s3_key=f"api://canvas/{course_id}")
            
            # The ingestion worker will handle extraction via CanvasAdapter
            result = worker.ingest(
                source_type="canvas",
                payload={
                    "course_id": course_id,
                    "university_id": university_id,
                    "author_id": author_id,
                    "force": force
                },
                task_id=task_id,
                on_progress=self._get_progress_callback(task_id)
            )
            
            if result.get("status") == "success":
                self._update_progress(task_id, "completed", "Canvas migration successful", 100)
                logger.info("Canvas API migration completed successfully", extra={"task_id": task_id, "course_id": course_id})
            else:
                self._update_progress(task_id, "failed", f"Migration failed: {result.get('error')}", 100)
                logger.error("Canvas API migration failed", extra={"task_id": task_id, "error": result.get("error")})
                
        except Exception as e:
            logger.error("Critical failure during Canvas migration", extra={"task_id": task_id, "error": str(e)})
            self._update_progress(task_id, "failed", str(e), 100)
