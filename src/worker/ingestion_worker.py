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
            institution = payload.get("institution", "")

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
                on_progress,
                institution=institution,
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
        on_progress,
        institution: str = "",
    ) -> Dict[str, Any]:
        """
        Runs the standard transformation and export pipeline.
        """
        # 1. Transform
        if on_progress: on_progress("transforming", 35, "Mapping to EduvateHub schema...")
        transformer = CourseTransformer()
        # For Blackboard exports the real course code lives in the BB course ID
        # (e.g. MGMT5306SPRING1ST8WKS2026VC01 → MGMT-5306), not the title.
        code_source = getattr(canvas_course, '_bb_course_id', None) or canvas_course.title
        transformed_course, report = transformer.transform(
            canvas_course,
            university_id,
            author_id,
            course_code=self._extract_course_code(code_source),
            department=self._extract_department(code_source)
        )

        # 2. Semantic Enrichment (bridges the bifurcation gap)
        # Runs the same intelligence as CanonicalPipeline/ContentEnricher
        # on the production LmsCourse so semantic metadata survives to MongoDB.
        if on_progress: on_progress("enriching", 55, "Applying semantic enrichment...")
        from core.enrichment import LmsCourseEnricher
        LmsCourseEnricher().enrich(transformed_course)

        # 3. Asset Migration
        if on_progress: on_progress("uploading_assets", 70, "Migrating assets to S3...")
        
        # If it's a zip source, we have a source_dir. If it's API, we might have remote URLs.
        source_dir = Path(canvas_course.source_directory) if hasattr(canvas_course, 'source_directory') else None
        
        uploader = AssetUploader(
            source_dir=source_dir, 
            s3_bucket=self.s3_bucket, 
            cdn_url=self.cdn_url,
            course_id=transformed_course.slug,
            institution=institution,
        )
        uploader.process_course_assets(transformed_course, canvas_course)

        # 4. Final Export
        if on_progress: on_progress("exporting", 90, "Saving to MongoDB...")
        
        from dataclasses import asdict
        course_dict = asdict(transformed_course)
        
        # Inject programId if needed for logical grouping (though not in target JSON)
        if program_id:
            course_dict["programId"] = program_id

        # Store institution code so validation reports can group by institution
        course_dict["institution_code"] = institution

        course_id = self.exporter.export(course_dict)
        
        # Track job in DB
        self.exporter.track_job(task_id, "N/A", "completed", course_id=course_id)

        # Auto-generate validation report immediately after ingestion
        self._run_post_ingestion_validation(course_id, canvas_course.title)

        return {
            "status": "success",
            "course_id": course_id,
            "title": canvas_course.title,
            "task_id": task_id
        }

    def _run_post_ingestion_validation(self, course_id: str, title: str) -> None:
        """
        Automatically runs the validation report after every successful ingestion.
        Saves HTML + JSON to storage/outputs/validation_<slug>.html
        """
        try:
            import sys
            from pathlib import Path
            scripts_dir = Path(__file__).parent.parent.parent / "scripts"
            if str(scripts_dir) not in sys.path:
                sys.path.insert(0, str(scripts_dir))
            from validate_ingestion import run_validation, save_report
            logger.info(f"Running post-ingestion validation for course {course_id}...")
            rep = run_validation(course_id, by_slug=False, strict=False, quiet=True)
            out_dir = Path(__file__).parent.parent.parent / "storage" / "outputs"
            html_path = save_report(rep, out_dir, emit_json=True)
            logger.info(f"Validation report saved: {html_path}",
                        extra={"verdict": rep.verdict.value, "manual_tasks": len(rep.manual_tasks)})
            msg = (f"\n[REPORT] Validation Report: {html_path}\n"
                   f"         {rep.verdict_label}\n")
            if rep.manual_tasks:
                msg += f"         {len(rep.manual_tasks)} manual task(s) required — see report for details.\n"
            sys.stdout.buffer.write(msg.encode("utf-8", errors="replace"))
            sys.stdout.buffer.flush()
        except Exception as e:
            logger.warning(f"Post-ingestion validation failed (non-fatal): {e}")

    def _discover_program_name(self, canvas_course) -> str:
        """Derive program name from course metadata or fallback."""
        return "General Onboarding"

    def _extract_course_code(self, title: str) -> str:
        """
        Extract a course code from the course title or Blackboard course ID.
        Handles patterns like:
          'IT-1104-01-25/FA'                    -> 'IT-1104'
          'PHI-1114 Logic...'                   -> 'PHI-1114'
          'CS 101 Intro...'                     -> 'CS-101'
          'Sandbox-IT-2620-Course-...'          -> 'IT-2620'
          'MGMT5306SPRING1ST8WKS2026VC01'       -> 'MGMT-5306'  (Blackboard course ID)
          'LEADERSHIP & MANAGEMENT DEVELOPMENT' -> scanned for embedded code
        """
        import re
        t = title.strip()

        # Primary: match at start of string  e.g. "IT-1104-01-25/FA"
        match = re.match(r'^([A-Z]{2,6}[-\s]\d{3,4})', t, re.IGNORECASE)
        if match:
            return match.group(1).replace(' ', '-').upper()

        # Blackboard course ID pattern: DEPT####TERM...  e.g. MGMT5306SPRING...
        match = re.match(r'^([A-Z]{2,6})(\d{3,4})', t, re.IGNORECASE)
        if match:
            return f"{match.group(1).upper()}-{match.group(2)}"

        # Scan anywhere in the title for a standard code pattern
        match = re.search(r'\b([A-Z]{2,6}[-]\d{3,4})\b', t, re.IGNORECASE)
        if match:
            return match.group(1).upper()

        return "IMPORTED"

    def _extract_department(self, title: str) -> str:
        """Derive department from course code prefix."""
        import re
        match = re.match(r'^([A-Z]{2,6})', title.strip(), re.IGNORECASE)
        if match:
            prefix = match.group(1).upper()
            dept_map = {
                "IT":   "Information Technology",
                "CS":   "Computer Science",
                "PHI":  "Philosophy",
                "ENT":  "Entrepreneurship",
                "BUS":  "Business",
                "ENG":  "English",
                "MAT":  "Mathematics",
                "SCI":  "Science",
                "MGMT": "Management",
                "MBA":  "Business Administration",
                "NURS": "Nursing",
                "EDUC": "Education",
                "PSYC": "Psychology",
                "HIST": "History",
                "BIOL": "Biology",
                "CHEM": "Chemistry",
            }
            return dept_map.get(prefix, prefix)
        return "Imported"

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
