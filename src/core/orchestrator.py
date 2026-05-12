"""
Job Orchestrator - State machine management for ingestion jobs.

Coordinates:
- State transitions with validation
- Checkpoint persistence
- Retry logic with backoff
- Recovery from partial failures
- Parallel stage execution coordination
"""

import asyncio
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from .job_state_machine import IngestionJob, JobState, JobCheckpoint
from .idempotency import IdempotencyService, IdempotencyKey, ContentHash
from .tracing import TracingMiddleware, get_correlation_id, TraceLogger
from utils.resilience import retry

logger = TraceLogger(__name__)


@dataclass
class StageDefinition:
    """Definition of a pipeline stage."""
    name: str
    state: JobState
    next_states: List[JobState]
    
    # Retry configuration
    retryable: bool = True
    max_retries: int = 3
    retry_delay_seconds: float = 5.0
    
    # Checkpoint configuration
    checkpoint: bool = True  # Should checkpoint after completion
    
    # Parallel configuration
    parallel: bool = False  # Can run in parallel with other stages
    dependencies: List[str] = field(default_factory=list)  # Required previous stages


class JobOrchestrator:
    """
    Orchestrates ingestion jobs with state machine and checkpointing.
    
    Responsibilities:
    1. Validate state transitions
    2. Persist job state
    3. Manage retries with backoff
    4. Enable recovery/resumption
    5. Coordinate parallel stage execution
    """
    
    # Pipeline stage definitions
    STAGES = [
        StageDefinition("received", JobState.RECEIVED, [JobState.CLASSIFYING]),
        StageDefinition("classifying", JobState.CLASSIFYING, [JobState.CLASSIFIED]),
        StageDefinition("classified", JobState.CLASSIFIED, [JobState.VALIDATING]),
        StageDefinition("validating", JobState.VALIDATING, [JobState.VALIDATED, JobState.FAILED]),
        StageDefinition("validated", JobState.VALIDATED, [JobState.RESOLVING]),
        StageDefinition("resolving", JobState.RESOLVING, [JobState.RESOLVED]),
        StageDefinition("resolved", JobState.RESOLVED, [JobState.PARSING]),
        StageDefinition("parsing", JobState.PARSING, [JobState.PARSED, JobState.FAILED]),
        StageDefinition("parsed", JobState.PARSED, [JobState.ENRICHING]),
        StageDefinition("enriching", JobState.ENRICHING, [JobState.ENRICHED]),
        StageDefinition("enriched", JobState.ENRICHED, [JobState.UPLOADING_ASSETS]),
        StageDefinition("uploading_assets", JobState.UPLOADING_ASSETS, [JobState.ASSETS_UPLOADED]),
        StageDefinition("assets_uploaded", JobState.ASSETS_UPLOADED, [JobState.EXPORTING]),
        StageDefinition("exporting", JobState.EXPORTING, [JobState.COMPLETED, JobState.FAILED]),
        StageDefinition("completed", JobState.COMPLETED, []),
        StageDefinition("failed", JobState.FAILED, [JobState.RETRYING]),
        StageDefinition("retrying", JobState.RETRYING, [JobState.CLASSIFYING]),  # Retry from classification
    ]
    
    def __init__(
        self,
        job_store=None,  # Job persistence layer
        idempotency_service: Optional[IdempotencyService] = None
    ):
        self.job_store = job_store or self._default_job_store()
        self.idempotency = idempotency_service or IdempotencyService()
        
        # State transition validation
        self._allowed_transitions = self._build_transition_map()
    
    def _build_transition_map(self) -> Dict[JobState, List[JobState]]:
        """Build valid state transition map."""
        transitions = {}
        for stage in self.STAGES:
            transitions[stage.state] = stage.next_states
        return transitions
    
    def create_job(
        self,
        job_id: str,
        source_type: str,
        source_path: str,
        source_metadata: Dict[str, Any] = None,
        correlation_id: Optional[str] = None
    ) -> IngestionJob:
        """Create a new ingestion job."""
        job = IngestionJob(
            job_id=job_id,
            correlation_id=correlation_id or str(uuid.uuid4()),
            source_type=source_type,
            source_path=source_path,
            source_metadata=source_metadata or {},
        )
        
        # Initial checkpoint
        job.checkpoint(
            stage=JobState.RECEIVED,
            progress=0,
            message="Job created",
            metadata={"source_type": source_type}
        )
        
        self._persist_job(job)
        logger.info("Job created", extra={"job_id": job_id})
        
        return job
    
    def transition_to(
        self,
        job_id: str,
        target_state: JobState,
        progress: int,
        message: str,
        metadata: Dict[str, Any] = None,
        artifacts: List[str] = None
    ) -> bool:
        """
        Attempt state transition with validation.
        
        Returns:
            True if transition succeeded, False if invalid
        """
        job = self.get_job(job_id)
        if not job:
            logger.error("Job not found for transition", extra={"job_id": job_id})
            return False
        
        # Validate transition
        allowed = self._allowed_transitions.get(job.state, [])
        if target_state not in allowed:
            logger.error(
                "Invalid state transition",
                extra={
                    "job_id": job_id,
                    "from": job.state.value,
                    "to": target_state.value,
                    "allowed": [s.value for s in allowed]
                }
            )
            return False
        
        # Execute transition
        job.checkpoint(
            stage=target_state,
            progress=progress,
            message=message,
            metadata=metadata,
            artifacts=artifacts
        )
        
        self._persist_job(job)
        logger.info("State transitioned", 
                   extra={"job_id": job_id, "state": target_state.value, "progress": progress})
        
        return True
    
    def mark_failed(self, job_id: str, error: str, exc: Exception = None):
        """Mark job as failed with error details."""
        job = self.get_job(job_id)
        if not job:
            return
        
        job.last_error = error
        job.error_count += 1
        
        # Check if retryable
        if job.is_retryable():
            logger.warning("Job failed - scheduling retry",
                          extra={"job_id": job_id, "retry_count": job.retry_count})
            # Schedule retry with backoff
            asyncio.create_task(self._schedule_retry(job))
        else:
            # No more retries - terminal failure
            job.state = JobState.FAILED
            job.checkpoint(
                stage=JobState.FAILED,
                progress=100,
                message=f"Failed: {error}"
            )
            logger.error("Job failed permanently", 
                        extra={"job_id": job_id, "errors": job.error_count})
        
        self._persist_job(job)
    
    async def _schedule_retry(self, job: IngestionJob):
        """Schedule a retry with exponential backoff."""
        job.retry_count += 1
        
        # Calculate delay
        delay = min(
            job.retry_policy.retry_delay_seconds * (job.retry_policy.backoff_multiplier ** (job.retry_count - 1)),
            job.retry_policy.max_delay_seconds
        )
        
        logger.info("Retry scheduled",
                   extra={"job_id": job.job_id, "attempt": job.retry_count, "delay_seconds": delay})
        
        await asyncio.sleep(delay)
        
        # Transition to retrying state
        job.checkpoint(
            stage=JobState.RETRYING,
            progress=job.progress_pct,
            message=f"Retry attempt {job.retry_count}/{job.retry_policy.max_attempts}"
        )
        self._persist_job(job)
        
        # Then transition back to appropriate stage (simplified: back to parsing)
        self.transition_to(
            job.job_id,
            JobState.CLASSIFYING,  # Restart from beginning for simplicity
            progress=10,
            message="Retrying from classification"
        )
    
    def get_job(self, job_id: str) -> Optional[IngestionJob]:
        """Retrieve job by ID."""
        data = self.job_store.get(job_id)
        if data:
            return IngestionJob.from_dict(data)
        return None
    
    def update_progress(self, job_id: str, progress: int, message: str):
        """Update job progress without state change."""
        job = self.get_job(job_id)
        if job:
            job.progress_pct = progress
            job.progress_message = message
            self._persist_job(job)
    
    def _persist_job(self, job: IngestionJob):
        """Persist job state."""
        self.job_store.save(job.to_dict())
    
    def _default_job_store(self):
        """Default in-memory job store (for testing)."""
        return InMemoryJobStore()
    
    def recover_job(self, job_id: str) -> Optional[IngestionJob]:
        """
        Attempt to recover a failed/incomplete job.
        
        Returns:
            Recovered job with last checkpoint, or None if not recoverable
        """
        job = self.get_job(job_id)
        if not job:
            return None
        
        if job.state in [JobState.COMPLETED, JobState.CANCELLED]:
            logger.info("Job already terminal", extra={"job_id": job_id, "state": job.state.value})
            return None
        
        # Can recover from any non-terminal state
        logger.info("Job recovered",
                   extra={"job_id": job_id, "state": job.state.value, "from_checkpoint": len(job.checkpoints)})
        
        return job


class InMemoryJobStore:
    """Simple in-memory job store for development."""
    
    def __init__(self):
        self._jobs: Dict[str, Dict[str, Any]] = {}
    
    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._jobs.get(job_id)
    
    def save(self, job_data: Dict[str, Any]):
        job_id = job_data["job_id"]
        self._jobs[job_id] = job_data
    
    def list_jobs(self, state: Optional[JobState] = None) -> List[Dict[str, Any]]:
        jobs = list(self._jobs.values())
        if state:
            jobs = [j for j in jobs if j.get("state") == state.value]
        return jobs
    
    def delete(self, job_id: str):
        self._jobs.pop(job_id, None)


class MongoDBJobStore:
    """MongoDB-backed persistent job store."""
    
    def __init__(self, mongodb_uri: Optional[str] = None):
        from exporters.mongodb_exporter import MongoDBExporter
        self.db = MongoDBExporter(mongodb_uri)
    
    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        self.db._ensure_connection()
        return self.db._db['ingestion_jobs'].find_one({"job_id": job_id})
    
    def save(self, job_data: Dict[str, Any]):
        self.db._ensure_connection()
        self.db._db['ingestion_jobs'].replace_one(
            {"job_id": job_data["job_id"]},
            job_data,
            upsert=True
        )
    
    def list_jobs(self, state: Optional[JobState] = None):
        self.db._ensure_connection()
        query = {}
        if state:
            query["state"] = state.value
        return list(self.db._db['ingestion_jobs'].find(query))