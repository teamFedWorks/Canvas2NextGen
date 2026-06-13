import datetime
from typing import Any, Dict, Optional

from src.ucae.workflow.state import JobState, JobEvent
from src.ucae.workflow.recovery import RecoveryArtifact


class JobPersistenceService:
    """
    Service to manage MongoDB jobs collection storage and state machine logging.
    Ensures that the event log remains append-only and provides atomic updates.
    """
    def __init__(self, db_client):
        self.db_client = db_client

    def _get_collection(self):
        db = self.db_client.get_database()
        return db.jobs

    def create_job(
        self, 
        job_id: str, 
        source_fingerprint: Optional[str] = None, 
        payload_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Initializes a new ingestion job entry in the database."""
        col = self._get_collection()
        now = datetime.datetime.utcnow()
        
        source_fp_doc = None
        if source_fingerprint:
            source_fp_doc = {
                "algorithm": "sha256",
                "version": 1,
                "value": source_fingerprint
            }

        job_doc = {
            "job_id": job_id,
            "sourceFingerprint": source_fp_doc,
            "status": JobState.CREATED.value,
            "created_at": now,
            "updated_at": now,
            "started_at": now,
            "ended_at": None,
            "duration_seconds": 0.0,
            "retries": 0,
            "attempts": [],
            "metadata": payload_metadata or {},
            "events": [
                JobEvent(
                    stage=JobState.CREATED.value,
                    message="Ingestion job registered in database.",
                    actor="system",
                    timestamp=now
                ).to_dict()
            ],
            "recovery_artifact": None,
            "provider_model_ref": None,
            "canonical_course_ref": None,
            "normalized_canonical_course_ref": None,
            "import_manifest_ref": None,
            "import_result_ref": None
        }
        col.update_one({"job_id": job_id}, {"$setOnInsert": job_doc}, upsert=True)

    def log_event(
        self, 
        job_id: str, 
        stage: str, 
        message: str, 
        actor: str = "system", 
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Atomically appends a JobEvent to the job record (append-only log)."""
        col = self._get_collection()
        now = datetime.datetime.utcnow()
        
        event = JobEvent(
            stage=stage,
            message=message,
            actor=actor,
            timestamp=now,
            metadata=details or {}
        )
        
        col.update_one(
            {"job_id": job_id},
            {
                "$push": {"events": event.to_dict()},
                "$set": {"updated_at": now}
            }
        )

    def save_context_references(self, job_id: str, context: Any) -> None:
        """Saves context artifact file references to the persistent job document."""
        col = self._get_collection()
        col.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "provider_model_ref": context.provider_model_ref,
                    "canonical_course_ref": context.canonical_course_ref,
                    "normalized_canonical_course_ref": context.normalized_canonical_course_ref,
                    "import_manifest_ref": context.import_manifest_ref,
                    "import_result_ref": context.import_result_ref,
                    "updated_at": datetime.datetime.utcnow()
                }
            }
        )

    def update_job_status(self, job_id: str, state: JobState, error_message: Optional[str] = None) -> None:
        """Updates the status and logs transition metrics."""
        col = self._get_collection()
        now = datetime.datetime.utcnow()
        
        update_fields: Dict[str, Any] = {
            "status": state.value,
            "updated_at": now
        }
        
        # Calculate duration if terminating
        if state in [JobState.SUCCESS, JobState.FAILED]:
            update_fields["ended_at"] = now
            # Find start time
            job_doc = col.find_one({"job_id": job_id})
            if job_doc and job_doc.get("started_at"):
                started = job_doc["started_at"]
                duration = (now - started).total_seconds()
                update_fields["duration_seconds"] = duration
                
        if error_message:
            update_fields["error_message"] = error_message
            
        col.update_one({"job_id": job_id}, {"$set": update_fields})
        
        self.log_event(
            job_id=job_id,
            stage=state.value,
            message=f"Job status transition to {state.value}.",
            details={"error": error_message} if error_message else None
        )

    def save_recovery_artifact(self, job_id: str, artifact: RecoveryArtifact) -> None:
        """Persists intermediate recovery artifact to the job record."""
        col = self._get_collection()
        col.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "recovery_artifact": artifact.to_dict(),
                    "updated_at": datetime.datetime.utcnow()
                }
            }
        )

    def load_recovery_artifact(self, job_id: str) -> Optional[RecoveryArtifact]:
        """Loads intermediate recovery artifact from the job record."""
        col = self._get_collection()
        doc = col.find_one({"job_id": job_id})
        if not doc or not doc.get("recovery_artifact"):
            return None
        
        art_dict = doc["recovery_artifact"]
        return RecoveryArtifact(
            schema_version=art_dict["schema_version"],
            provider_version=art_dict.get("provider_version", "1.0"),
            serialization_version=art_dict["serialization_version"],
            payload=art_dict.get("payload", art_dict.get("compressed_payload", "")),
            compression=art_dict.get("compression", "gzip")
        )

    def save_detection_diagnostics(self, job_id: str, diagnostics: list) -> None:
        """Saves provider detection diagnostics to the job document."""
        col = self._get_collection()
        col.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "detection_diagnostics": diagnostics,
                    "updated_at": datetime.datetime.utcnow()
                }
            }
        )

    def save_stage_metrics(self, job_id: str, stage_metrics: dict) -> None:
        """Saves stage duration metrics to the job document."""
        col = self._get_collection()
        col.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "stage_metrics": stage_metrics,
                    "updated_at": datetime.datetime.utcnow()
                }
            }
        )

    def record_failed_attempt(self, job_id: str, worker_id: str, error_message: str) -> None:
        """Appends a failed execution attempt to the attempts history array."""
        col = self._get_collection()
        now = datetime.datetime.utcnow()
        col.update_one(
            {"job_id": job_id},
            {
                "$push": {
                    "attempts": {
                        "timestamp": now,
                        "worker": worker_id,
                        "error": error_message
                    }
                },
                "$set": {
                    "updated_at": now
                }
            }
        )
