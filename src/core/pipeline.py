"""
Production Canvas -> Custom LMS Migration Pipeline

This is the main entry point for the migration pipeline.
Orchestrates: Validation, Parsing, Transformation, Asset Upload, and DB Write.
"""

import time
from pathlib import Path
from typing import Optional

from models.migration_report import MigrationReport, ReportStatus
from .stages.validator import Validator
from .stages.parser import Parser
from .stages.pre_ingest_audit import PreIngestAuditor
from transformers.course_transformer import CourseTransformer
from .stages.asset_uploader import AssetUploader
from exporters.mongodb_exporter import MongoDBExporter
from observability.logger import get_logger

logger = get_logger(__name__)


class MigrationPipeline:
    """
    Orchestrates the Canvas -> Custom LMS migration flow.
    """
    
    def __init__(
        self, 
        course_directory: Path, 
        output_directory: Optional[Path] = None, 
        on_progress=None,
        university_id: Optional[str] = None,
        author_id: Optional[str] = None,
        course_code: Optional[str] = None,
        task_id: Optional[str] = None
    ):
        """
        Initialize the migration pipeline.
        """
        self.course_directory = Path(course_directory)
        self.university_id = university_id
        self.author_id = author_id
        self.on_progress = on_progress
        self.output_directory = Path(output_directory) if output_directory else self.course_directory / "lms_output"
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        self.university_id = university_id
        self.author_id = author_id
        self.course_code = course_code
        self.task_id = task_id or f"internal_{int(time.time())}"
        
        self.report = MigrationReport(
            status=ReportStatus.SUCCESS,
            source_directory=str(self.course_directory),
            output_directory=str(self.output_directory)
        )
    
    def run(self) -> MigrationReport:
        """
        Execute the sequential pipeline stages.
        """
        start_time = time.time()
        logger.info("Starting Migration Pipeline", extra={"source": str(self.course_directory)})
        
        try:
            # Stage 0: Pre-Ingest Audit (Fix 5)
            # Fast manifest scan — sets operator expectations before any heavy work.
            self._notify("auditing", 5, "Running pre-ingest audit...")
            auditor = PreIngestAuditor(self.course_directory)
            audit_report = auditor.audit()
            self.report.pre_ingest_audit = audit_report.to_dict()

            # Stage 1: Validation
            self._notify("validating", 10, "Validating Canvas package...")
            validator = Validator(self.course_directory)
            validation_report = validator.validate()
            self.report.validation_report = validation_report
            
            if not validation_report.passed:
                logger.error("Validation failed", extra={"errors": validation_report.errors})
                self.report.status = ReportStatus.FAILURE
                return self._finalize(start_time)
            
            # Stage 2: Parsing
            self._notify("parsing", 30, "Parsing course content...")
            parser = Parser(self.course_directory)
            canvas_course, parse_report = parser.parse()
            self.report.parse_report = parse_report
            
            if not canvas_course:
                logger.error("Parsing failed")
                self.report.status = ReportStatus.FAILURE
                return self._finalize(start_time)
            
            self.report.source_course_title = canvas_course.title
            self.report.source_content_counts = canvas_course.get_content_counts()
            
            # Stage 3: Transformation
            self._notify("transforming", 50, "Transforming to LMS models...")
            transformer = CourseTransformer()
            lms_course, transformation_report = transformer.transform(
                canvas_course,
                university_id=self.university_id,
                author_id=self.author_id,
                course_code=self.course_code
            )
            self.report.transformation_report = transformation_report
            
            # Stage 4: Asset Upload & URL Rewriting
            self._notify("uploading_assets", 70, "Uploading assets to S3...")
            import os
            s3_bucket = os.getenv("S3_CDN_BUCKET", "uhub-lms-bucket")
            cdn_url = os.getenv("CDN_URL", "")
            
            uploader = AssetUploader(
                course_id=lms_course.slug,
                source_dir=self.course_directory,
                s3_bucket=s3_bucket,
                cdn_url=cdn_url
            )
            lms_course = uploader.process_course_assets(lms_course)
            
            # Stage 5: Database Write
            self._notify("exporting", 90, "Writing to MongoDB...")
            import os
            mongo_uri = os.getenv("ULCP_MONGODB_URI")
            db_name = os.getenv("ULCP_MONGODB_DATABASE", "test")
            db_writer = MongoDBExporter(mongodb_uri=mongo_uri, database_name=db_name)
            
            from dataclasses import asdict
            course_dict = asdict(lms_course)
            
            # Inject Canvas ID for tracking
            if lms_course.canvas_course_id:
                course_dict["canvas_course_id"] = lms_course.canvas_course_id

            course_id = db_writer.export(course_dict)
            
            # Track job in DB
            db_writer.track_job(self.task_id, "N/A", "completed", course_id=course_id)
            
            self.report.migrated_content_counts = lms_course.get_content_counts()
            logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.exception("Pipeline crashed")
            self.report.status = ReportStatus.FAILURE
        
        return self._finalize(start_time)

    def _notify(self, stage: str, progress: int, message: str):
        """Execute progress callback."""
        logger.info(f"Pipeline Stage: {stage} - {message}")
        if self.on_progress:
            self.on_progress(stage, progress, message)

    def _finalize(self, start_time: float) -> MigrationReport:
        """Finalize metrics."""
        self.report.execution_time_seconds = time.time() - start_time
        self.report.aggregate_errors()
        return self.report

    def _notify(self, stage: str, progress: int, message: str):
        """Execute progress callback."""
        logger.info(f"Pipeline Stage: {stage} - {message}")
        if self.on_progress:
            self.on_progress(stage, progress, message)

    def _finalize(self, start_time: float) -> MigrationReport:
        """Finalize metrics."""
        self.report.execution_time_seconds = time.time() - start_time
        self.report.aggregate_errors()
        return self.report
