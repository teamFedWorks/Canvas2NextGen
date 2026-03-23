"""
Production Canvas -> Custom LMS Migration Pipeline

This is the main entry point for the migration pipeline.
Orchestrates: Validation, Parsing, Transformation, Asset Upload, and DB Write.
"""

import time
from pathlib import Path
from typing import Optional, Any, List

from ..models.migration_report import MigrationReport, ReportStatus
from ..models.lms_models import LmsCourse
from .stages.validator import Validator
from .stages.parser import Parser
from ..transformers.course_transformer import CourseTransformer
from .stages.asset_uploader import AssetUploader
from ..exporters.mongodb_uploader import MongoDBUploader
from ..observability.logger import get_logger

logger = get_logger(__name__)


class MigrationPipeline:
    """
    Orchestrates the Canvas → Custom LMS migration flow.
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
        
        # State for resumability
        self.canvas_course: Optional[Any] = None
        self.lms_course: Optional[LmsCourse] = None
    
    def run(self) -> MigrationReport:
        """
        Execute the sequential pipeline stages with resumability support.
        """
        start_time = time.time()
        logger.info("Starting Migration Pipeline", extra={"task_id": self.task_id, "source": str(self.course_directory)})
        
        # 0. Check for Resume Point
        db_writer = MongoDBUploader()
        existing_job = db_writer._col_jobs.find_one({"_id": self.task_id}) if db_writer._col_jobs is not None else None
        
        last_stage: Optional[str] = None
        if existing_job and isinstance(existing_job.get("current_stage"), str):
            last_stage = existing_job.get("current_stage")
        
        stages = ["validating", "parsing", "transforming", "uploading_assets", "exporting"]
        current_stage_idx = 0
        if last_stage in stages:
            current_stage_idx = stages.index(last_stage) + 1
        
        try:
            # Stage 1: Validation
            if current_stage_idx <= 0:
                self._notify("validating", 10, "Validating Canvas package...")
                db_writer.update_job_status(self.task_id, "processing", current_stage="validating", progress=10)
                validator = Validator(self.course_directory)
                validation_report = validator.validate()
                self.report.validation_report = validation_report
                
                if not validation_report.passed:
                    logger.error("Validation failed", extra={"errors": validation_report.errors})
                    self.report.status = ReportStatus.FAILURE
                    return self._finalize(start_time)
            
            # Stage 2: Parsing
            if current_stage_idx <= 1:
                self._notify("parsing", 30, "Parsing course content...")
                db_writer.update_job_status(self.task_id, "processing", current_stage="parsing", progress=30)
                parser = Parser(self.course_directory)
                canvas_course, parse_report = parser.parse()
                self.report.parse_report = parse_report
                
                if not canvas_course:
                    logger.error("Parsing failed")
                    self.report.status = ReportStatus.FAILURE
                    return self._finalize(start_time)
                
                self.report.source_course_title = canvas_course.title
                self.report.source_content_counts = canvas_course.get_content_counts()
                self.canvas_course = canvas_course # Save for next stage
            else:
                # Need to re-parse or load from cache if we skip parsing
                # For now, we assume parsing is cheap enough to re-run if we stalled at transform
                # But ideally we'd cache the parsed Canvas course
                parser = Parser(self.course_directory)
                self.canvas_course, _ = parser.parse()

            # Stage 3: Transformation
            if current_stage_idx <= 2:
                self._notify("transforming", 50, "Transforming to LMS models...")
                db_writer.update_job_status(self.task_id, "processing", current_stage="transforming", progress=50)
                transformer = CourseTransformer()
                lms_course, transformation_report = transformer.transform(
                    self.canvas_course,
                    university_id=self.university_id,
                    author_id=self.author_id,
                    course_code=self.course_code
                )
                self.report.transformation_report = transformation_report
                self.lms_course = lms_course
            
            # Stage 4: Asset Upload & URL Rewriting
            if current_stage_idx <= 3:
                self._notify("uploading_assets", 70, "Uploading assets to S3...")
                db_writer.update_job_status(self.task_id, "processing", current_stage="uploading_assets", progress=70)
                
                import os
                s3_bucket = os.getenv("S3_ASSETS_BUCKET", "lms-course-assets")
                cdn_url = os.getenv("S3_CDN_BASE_URL", "")
                
                # Load existing asset checkpoints if any
                pre_uploaded = existing_job.get("uploaded_assets", {}) if existing_job else {}
                
                uploader = AssetUploader(
                    course_id=self.lms_course.canvas_course_id,
                    source_dir=self.course_directory,
                    s3_bucket=s3_bucket,
                    cdn_base_url=cdn_url,
                    pre_uploaded_assets=pre_uploaded
                )
                self.lms_course = uploader.process_course(self.lms_course)
                
                # Save checkpoint after assets are done
                db_writer._col_jobs.update_one(
                    {"_id": self.task_id},
                    {"$set": {"uploaded_assets": uploader.uploaded_assets}}
                )
            
            # Stage 5: Database Write
            if current_stage_idx <= 4:
                self._notify("exporting", 90, "Writing to MongoDB...")
                db_writer.update_job_status(self.task_id, "processing", current_stage="exporting", progress=90)
                
                # Collect warnings for job record
                all_warnings = []
                if self.report.transformation_report:
                    all_warnings.extend(self.report.transformation_report.warnings)

                success = db_writer.write_lms_course(self.lms_course, self.task_id, warnings=all_warnings)
                
                if not success:
                    logger.error("Database write failed")
                    self.report.status = ReportStatus.FAILURE
                else:
                    self.report.migrated_content_counts = self.lms_course.get_content_counts()
                    logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.exception("Pipeline crashed")
            self.report.status = ReportStatus.FAILURE
            db_writer.update_job_status(self.task_id, "failed", log_msg=f"Pipeline Crash: {str(e)}")
        
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
