import os
import shutil
import tempfile
import zipfile
import uuid
from pathlib import Path
from typing import Dict, Any, Optional

from ..core.stages.package_validator import PackageValidator
from ..core.pipeline import MigrationPipeline
from ..exporters.mongodb_uploader import MongoDBUploader
from ..observability.logger import get_logger

logger = get_logger(__name__)

class IngestionWorker:
    """
    Orchestrates the standardized ingestion pipeline, bridging the legacy 
    worker interface to the modern MigrationPipeline.
    """

    def __init__(self, s3_bucket: Optional[str] = None, cdn_url: Optional[str] = None):
        self.s3_bucket = s3_bucket or os.getenv("S3_ASSETS_BUCKET", "lms-course-assets")
        self.cdn_url = cdn_url or os.getenv("S3_CDN_BASE_URL", "")
        self.validator = PackageValidator()
        self.db_writer = MongoDBUploader()

    def process_package(
        self, 
        zip_path: Path, 
        university_id: str, 
        author_id: str, 
        program_name: str = None, 
        title_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Runs the consolidated pipeline with full job tracking and security validation.
        """
        task_id = str(uuid.uuid4())
        logger.log("INFO", "Starting standardized ingestion task", 
                   task_id=task_id, 
                   filename=zip_path.name)
        
        # 1. Idempotency Check
        checksum = self.validator.calculate_checksum(zip_path)
        existing_job = self.db_writer.find_by_checksum(checksum)
        
        if existing_job and existing_job.get('status') == 'completed':
            return {
                "status": "success", 
                "course_id": existing_job.get('course_id'), 
                "message": "This file was already imported.",
                "reused": True
            }

        # 2. Initialize Job record
        self.db_writer.create_job(task_id, s3_key=zip_path.name, checksum=checksum)

        # 3. Security Validation
        is_valid, msg = self.validator.validate_zip(zip_path)
        if not is_valid:
            self.db_writer.update_job_status(task_id, "failed", log_msg=msg)
            return {"status": "failed", "error": msg}

        # 4. Extract to temp directory
        extract_dir = Path(tempfile.mkdtemp(prefix="lms_ingest_"))
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # 5. Hand over to modern MigrationPipeline
            # We wrap the pipeline to use the same task_id for unified tracking
            pipeline = MigrationPipeline(
                course_directory=extract_dir,
                university_id=university_id,
                author_id=author_id,
                task_id=task_id,
                on_progress=lambda stage, prog, msg: self.db_writer.update_job_status(
                    task_id, "processing", log_msg=msg, progress=prog
                )
            )

            report = pipeline.run()

            if report.status.value in ("failure", "partial_failure"):
                # Job status is updated within pipeline via on_progress and MongoDBUploader.write_lms_course
                return {
                    "status": "failed", 
                    "error": "Pipeline execution failed. Check job logs for details.",
                    "task_id": task_id
                }

            return {
                "status": "success",
                "course_id": report.source_course_title, # Note: Return real ID if available
                "task_id": task_id,
                "report": report.get_summary_dict()
            }

        except Exception as e:
            logger.log("ERROR", "Worker execution crash", task_id=task_id, error=str(e))
            self.db_writer.update_job_status(task_id, "failed", log_msg=f"Worker Crash: {str(e)}")
            raise e
        finally:
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            self.db_writer.close()
