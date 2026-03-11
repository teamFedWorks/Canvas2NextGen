import os
import shutil
import zipfile
from pathlib import Path
from fastapi import UploadFile
import tempfile
import time
from typing import Dict, Any, Optional

from Canvas_Converter import MigrationPipeline
from src.models.migration_report import ReportStatus

class MigrationService:
    def __init__(self):
        self.storage_dir = Path(os.getenv("STORAGE_DIR", "storage"))
        self.uploads_dir = self.storage_dir / "uploads"
        self.outputs_dir = self.storage_dir / "outputs"
        
        # Ensure directories exist
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)
        
        # Tasks in-memory for this simple implementation
        # (In production, this would be in MongoDB)
        self.tasks: Dict[str, Dict[str, Any]] = {}

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self.tasks.get(task_id)

    async def process_migration(self, task_id: str, file: UploadFile):
        """
        Background task to process the migration.
        """
        import time
        self.tasks[task_id] = {
            "status": "processing",
            "progress": 0,
            "message": "Initializing...",
            "current_step": "extracting",
            "steps": [
                {"id": "extracting", "label": "Extracting Files", "status": "pending"},
                {"id": "validating", "label": "Validating Structure", "status": "pending"},
                {"id": "parsing", "label": "Parsing Content", "status": "pending"},
                {"id": "transforming", "label": "Transforming Data", "status": "pending"},
                {"id": "exporting", "label": "Exporting JSON", "status": "pending"},
                {"id": "finalizing", "label": "Finalizing Reports", "status": "pending"}
            ],
            "started_at": os.times()[4]
        }

        def on_pipeline_progress(step_id, progress, message):
            self.tasks[task_id]["message"] = message
            self.tasks[task_id]["progress"] = progress
            self.tasks[task_id]["current_step"] = step_id
            
            # Update step statuses
            found_current = False
            for step in self.tasks[task_id]["steps"]:
                if step["id"] == step_id:
                    step["status"] = "active"
                    found_current = True
                elif not found_current:
                    step["status"] = "completed"
                else:
                    step["status"] = "pending"
            
            # Artificial delay for visual feedback of stage-by-stage progress
            time.sleep(0.8)
        
        temp_dir = Path(tempfile.mkdtemp(prefix=f"migration_{task_id}_"))
        
        try:
            # 1. Save uploaded ZIP
            self.tasks[task_id]["message"] = "Saving ZIP file..."
            zip_path = self.uploads_dir / f"{task_id}.zip"
            with open(zip_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            # 2. Extract ZIP
            on_pipeline_progress("extracting", 5, "Extracting ZIP contents...")
            extract_dir = temp_dir / "source"
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # 3. Initialize Pipeline
            output_dir = self.outputs_dir / task_id
            pipeline = MigrationPipeline(extract_dir, output_dir, on_progress=on_pipeline_progress)
            
            # 4. Run Pipeline
            report = pipeline.run()
            
            # 5. Finalize Task Status
            is_success = report.status == ReportStatus.SUCCESS
            self.tasks[task_id]["status"] = "completed" if is_success else "failed"
            self.tasks[task_id]["message"] = "Migration finished."
            
            # Mark all steps as completed if overall success
            if is_success:
                for step in self.tasks[task_id]["steps"]:
                    step["status"] = "completed"
            
            self.tasks[task_id]["report"] = {
                "status": report.status.value,
                "course": report.source_course_title,
                "output_path": str(output_dir),
                "summary": report.get_summary_dict(),
                "source_counts": report.source_content_counts,
                "counts": report.migrated_content_counts,
                "total_errors": report.total_errors,
                "total_warnings": report.total_warnings,
                "execution_time": round(report.execution_time_seconds, 2)
            }
            
        except Exception as e:
            self.tasks[task_id]["status"] = "failed"
            self.tasks[task_id]["message"] = f"Error: {str(e)}"
            # Mark current step as failed
            current_step = self.tasks[task_id].get("current_step")
            for step in self.tasks[task_id]["steps"]:
                if step["id"] == current_step:
                    step["status"] = "failed"
        
        finally:
            # Cleanup temp source files (we keep the output in storage)
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            # We can also delete the zip_path if needed
