"""
Ingestion Worker - Orchestrates the multi-source ingestion platform.

Supports ZIP (IMSCC) and Direct API (Canvas) ingestion through 
a normalized adapter strategy.
"""

import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, Callable

from adapters.zip_adapter import ZipAdapter
from adapters.canvas_adapter import CanvasAdapter
from transformers.course_transformer import CourseTransformer
from core.stages.asset_uploader import AssetUploader
from exporters.mongodb_exporter import MongoDBExporter
from observability.logger import get_logger

logger = get_logger(__name__)

class IngestionWorker:
    """
    The central engine for course ingestion.
    Dispatches to adapters and runs the normalized migration pipeline.
    """

    def __init__(self, s3_bucket: str, cdn_url: str):
        self.s3_bucket = s3_bucket
        self.cdn_url = cdn_url
        self.exporter = MongoDBExporter()
        self.default_uni = os.getenv("DEFAULT_UNIVERSITY_ID", "default_univ")
        self.default_author = os.getenv("DEFAULT_AUTHOR_ID", "default_author")
        
        # Register Adapters
        self.adapters = {
            "zip": ZipAdapter(),
            "canvas": CanvasAdapter()
        }

    def ingest(
        self, 
        source_type: str, 
        payload: Dict[str, Any], 
        task_id: Optional[str] = None,
        on_progress: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Primary entry point for any course ingestion.
        """
        task_id = task_id or str(uuid.uuid4())
        logger.info(f"Starting ingestion task [{task_id}] from source: {source_type}")
        
        if source_type not in self.adapters:
            return {"status": "failed", "error": f"Unsupported source type: {source_type}"}

        adapter = self.adapters[source_type]
        canvas_course = None
        
        try:
            # 1. Load & Parse through Adapter
            if on_progress: on_progress("extracting", 10, f"Extracting from {source_type}...")
            canvas_course = adapter.load(payload)
            
            # 2. Logic for Program Discovery & Scoping
            university_id = payload.get("university_id", self.default_uni)
            author_id = payload.get("author_id", self.default_author)
            force = payload.get("force", False)
            
            # Use provided program_name or derive it
            program_name = payload.get("program_name") or self._discover_program_name(canvas_course)
            program_id = self.exporter.get_or_create_program(university_id, program_name)

            # 3. Deduplication Check
            if not force:
                existing_id = self.exporter.check_logical_duplicate(
                    university_id, 
                    program_id, 
                    canvas_course.title,
                    canvas_course_id=canvas_course.identifier
                )
                if existing_id:
                    logger.info(f"Course '{canvas_course.title}' already exists. Skipping.")
                    return {
                        "status": "success", 
                        "course_id": existing_id, 
                        "message": "Course already exists.",
                        "deduplicated": True
                    }

            # 4. Pipeline Execution
            return self._run_pipeline(
                canvas_course, 
                university_id, 
                program_id, 
                author_id, 
                task_id, 
                on_progress
            )

        except Exception as e:
            logger.exception(f"Ingestion failed for task {task_id}")
            return {"status": "failed", "error": str(e)}
        finally:
            # Cleanup source directory if it was a temp extraction
            if hasattr(canvas_course, 'source_directory') and canvas_course.source_directory:
                src_dir = Path(canvas_course.source_directory)
                if src_dir.exists() and "lms_zip_extract_" in src_dir.name:
                    shutil.rmtree(src_dir)
        
        return {"status": "failed", "error": "Unknown ingestion error"}

    def _run_pipeline(
        self, 
        canvas_course, 
        university_id, 
        program_id, 
        author_id, 
        task_id, 
        on_progress
    ) -> Dict[str, Any]:
        """
        Runs the standard transformation and export pipeline.
        """
        # 1. Transform
        if on_progress: on_progress("transforming", 40, "Mapping to EduvateHub schema...")
        transformer = CourseTransformer()
        transformed_course, report = transformer.transform(
            canvas_course, 
            university_id, 
            author_id
            # course_code can be added here if available in adapter
        )

        # 2. Asset Migration
        if on_progress: on_progress("uploading_assets", 70, "Migrating assets to S3...")
        
        # If it's a zip source, we have a source_dir. If it's API, we might have remote URLs.
        source_dir = Path(canvas_course.source_directory) if hasattr(canvas_course, 'source_directory') else None
        
        uploader = AssetUploader(
            source_dir=source_dir, 
            s3_bucket=self.s3_bucket, 
            cdn_url=self.cdn_url,
            course_id=transformed_course.slug
        )
        uploader.process_course_assets(transformed_course)

        # 3. Final Export
        if on_progress: on_progress("exporting", 90, "Saving to MongoDB...")
        
        from dataclasses import asdict
        course_dict = asdict(transformed_course)
        
        # Inject programId if needed for logical grouping (though not in target JSON)
        if program_id:
            course_dict["programId"] = program_id
            
        course_id = self.exporter.export(course_dict)
        
        # Track job in DB
        self.exporter.track_job(task_id, "N/A", "completed", course_id=course_id)

        return {
            "status": "success",
            "course_id": course_id,
            "title": canvas_course.title,
            "task_id": task_id
        }

    def _discover_program_name(self, canvas_course) -> str:
        """Derive program name from course metadata or fallback."""
        return "General Onboarding"

    def process_package(self, zip_path: Path, university_id: str, author_id: str, **kwargs) -> Dict[str, Any]:
        """Legacy compatibility for batch scripts."""
        return self.ingest(
            source_type="zip",
            payload={
                "zip_path": zip_path,
                "university_id": university_id,
                "author_id": author_id,
                **kwargs
            }
        )
