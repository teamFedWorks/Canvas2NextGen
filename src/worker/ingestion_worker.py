import os
import shutil
import tempfile
import zipfile
import uuid
from pathlib import Path
from typing import Dict, Any, Optional

from ..validation.package_validator import PackageValidator
from ..detection.format_detector import FormatDetector, ExportFormat
from ..parsers.imscc_parser import IMSCCParser
from ..parsers.canvas_export_parser import CanvasExportParser
from ..transformers.course_transformer import CourseTransformer
from ..assets.asset_uploader import AssetUploader
from ..exporters.mongodb_exporter import MongoDBExporter
from ..utils.logger import get_logger

logger = get_logger(__name__)

class IngestionWorker:
    """
    Orchestrates the hardened ingestion pipeline.
    """

    def __init__(self, s3_bucket: str, cdn_url: str):
        self.s3_bucket = s3_bucket
        self.cdn_url = cdn_url
        self.validator = PackageValidator()
        self.exporter = MongoDBExporter()

    def process_package(self, zip_path: Path, university_id: str, author_id: str) -> Dict[str, Any]:
        """
        Runs the pipeline for a single course package with full hardening.
        """
        task_id = str(uuid.uuid4())
        logger.log("INFO", "Starting ingestion task", 
                   task_id=task_id, 
                   filename=zip_path.name)
        
        # 1. Idempotency Check (Checksum)
        checksum = self.validator.calculate_checksum(zip_path)
        existing_job = self.exporter.find_by_checksum(checksum)
        
        if existing_job and existing_job.get('status') == 'completed':
            logger.log("INFO", "Duplicate package detected, skipping ingestion", 
                       checksum=checksum, 
                       course_id=existing_job.get('course_id'))
            return {
                "status": "success", 
                "course_id": existing_job.get('course_id'), 
                "message": "Duplicate package already imported.",
                "reused": True
            }

        # Track job start
        self.exporter.track_job(task_id, checksum, "processing")

        # 2. Security Validation
        is_valid, msg = self.validator.validate_zip(zip_path)
        if not is_valid:
            logger.log("ERROR", "Validation failed", task_id=task_id, error=msg)
            self.exporter.track_job(task_id, checksum, "failed")
            return {"status": "failed", "error": msg}

        # 3. Extract to temp directory
        extract_dir = Path(tempfile.mkdtemp(prefix="lms_ingest_"))
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # 4. Detect Format
            fmt = FormatDetector.detect(extract_dir)
            if fmt == ExportFormat.UNKNOWN:
                logger.log("ERROR", "Unknown format", task_id=task_id)
                self.exporter.track_job(task_id, checksum, "failed")
                return {"status": "failed", "error": "Unknown export format."}

            logger.log("INFO", "Format detected", task_id=task_id, format=fmt.value)

            # 5. Parse
            if fmt == ExportFormat.IMSCC:
                parser = IMSCCParser(extract_dir)
            else:
                parser = CanvasExportParser(extract_dir)
            
            parsed_data = parser.parse()
            if "error" in parsed_data:
                logger.log("ERROR", "Parsing failed", task_id=task_id, error=parsed_data["error"])
                self.exporter.track_job(task_id, checksum, "failed")
                return {"status": "failed", "error": parsed_data["error"]}

            # 6. Transform
            transformer = CourseTransformer()
            transformed_course = transformer.transform(parsed_data, university_id, author_id)

            # 7. Process Assets (Parallel S3 + Rewrite)
            logger.log("INFO", "Processing assets", task_id=task_id)
            uploader = AssetUploader(extract_dir, self.s3_bucket, self.cdn_url)
            uploader.process_course_assets(transformed_course)

            # 8. Export to MongoDB (with size check and retries)
            course_id = self.exporter.export(transformed_course)

            # Mark job complete
            self.exporter.track_job(task_id, checksum, "completed", course_id=course_id)

            logger.log("INFO", "Ingestion completed successfully", 
                       task_id=task_id, 
                       course_id=course_id)

            return {
                "status": "success",
                "course_id": course_id,
                "title": transformed_course["title"],
                "format": fmt.value,
                "task_id": task_id
            }

        except Exception as e:
            logger.log("ERROR", "Pipeline crash", task_id=task_id, error=str(e))
            self.exporter.track_job(task_id, checksum, "failed")
            return {"status": "failed", "error": f"Internal pipeline error: {str(e)}"}
        finally:
            # Cleanup temp files
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            self.exporter.close()
