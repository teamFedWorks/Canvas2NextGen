import boto3
import json
import logging
import time
import datetime
import uuid
import os
from typing import Dict, Any, Optional

from src.ucae.workflow.lock import MongoLockManager
from src.ucae.workflow.exceptions import LockAcquisitionError

from src.observability.tracing import TraceLogger
logger = TraceLogger(__name__)


class PromotionQueueListener:
    """
    polls SQS FIFO queue for promotion messages, verifies idempotency against
    promotion_executions, acquires promote_{courseId} locks, promotes course 
    document from ULCP to Platform, and appends to promotion_events.
    """
    def __init__(
        self,
        queue_url: str,
        ulcp_db_client,
        platform_db_client,
        sqs_client=None,
        visibility_timeout_secs: int = 120,
        worker_id: str = "promotion_worker_1"
    ):
        self.queue_url = queue_url
        self.ulcp_db_client = ulcp_db_client
        self.platform_db_client = platform_db_client
        self.sqs_client = sqs_client or boto3.client("sqs")
        self.visibility_timeout_secs = visibility_timeout_secs
        self.worker_id = worker_id

    def poll_messages(self, max_messages: int = 1, wait_time_seconds: int = 10) -> None:
        """Polls SQS FIFO queue for promotion requests and processes them."""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                AttributeNames=["ApproximateReceiveCount"]
            )
        except Exception as e:
            logger.error(f"Error receiving promotion messages from SQS: {e}")
            return

        messages = response.get("Messages", [])
        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1))
            
            try:
                self._process_message(message, receive_count)
                
                # Success -> delete message
                self.sqs_client.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
            except LockAcquisitionError as e:
                logger.warning(f"Promotion lock acquisition failed: {e}. Leaving in SQS for retry.")
                from src.observability.metrics import metrics
                metrics.emit_metric("PromotionLockFailed")
            except Exception as e:
                logger.exception(f"Failed to process promotion request: {message['MessageId']}")
                from src.observability.metrics import metrics
                metrics.emit_metric("PromotionFailed")
                # Shorten visibility timeout on failure for retry
                try:
                    self.sqs_client.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=10
                    )
                except Exception:
                    pass

    def _process_message(self, message: Dict[str, Any], receive_count: int) -> None:
        body = json.loads(message["Body"])
        job_id = body.get("job_id")
        course_id = body.get("course_id")
        correlation_id = body.get("correlation_id") or str(uuid.uuid4())
        
        import time
        start_time = time.time()
        from src.observability.metrics import metrics
        metrics.emit_metric("PromotionStarted")

        if not job_id or not course_id:
            raise ValueError(f"Invalid promotion message body, job_id/course_id missing: {body}")

        # Connect to databases
        ulcp_db = self.ulcp_db_client.get_database()
        platform_db = self.platform_db_client.get_database()

        # 1. Fetch content fingerprint to act as idempotency key
        dedup_id = body.get("deduplication_id")
        
        job = ulcp_db.jobs.find_one({"job_id": job_id})
        fingerprint = None
        if job:
            fingerprint = job.get("content_fingerprint") or job.get("source_metadata", {}).get("content_fingerprint")
        
        if not fingerprint:
            # Try to query the course itself
            course_doc = ulcp_db.courses.find_one({"slug": course_id}) or ulcp_db.courses.find_one({"_id": course_id})
            if course_doc and course_doc.get("content_fingerprint"):
                fingerprint = course_doc["content_fingerprint"]
            else:
                # Fallback deterministic key if normalizer fingerprint not present
                fingerprint = f"fp_{course_id}_{job_id}"

        # Standardize dedup_id calculation
        from src.core.idempotency import build_promotion_dedup_id
        dedup_id = build_promotion_dedup_id(course_id, fingerprint)

        logger.info(f"Processing promotion for course: {course_id}, job: {job_id}, fingerprint: {fingerprint}, deduplication_id: {dedup_id}")

        # 2. Check Idempotency Key in promotion_executions
        existing_exec = platform_db.promotion_executions.find_one({"_id": dedup_id})
        if existing_exec:
            logger.info(f"Idempotency match: promotion for deduplication key {dedup_id} already completed. Skipping promotion.")
            
            # Log event to promotion_events anyway for auditability
            self._log_promotion_event(
                platform_db=platform_db,
                dedup_id=dedup_id,
                fingerprint=fingerprint,
                job_id=job_id,
                course_id=course_id,
                correlation_id=correlation_id,
                stage="SKIPPED",
                message=f"Promotion message replayed and skipped due to deduplication key {dedup_id}."
            )
            return

        # 3. Acquire Distributed lock on platform DB to prevent concurrent writes for same course
        lock_id = f"promote_{course_id}"
        lock_manager = MongoLockManager(self.platform_db_client)
        if not lock_manager.acquire_lock(lock_id=lock_id, worker_id=self.worker_id, lease_secs=120):
            raise LockAcquisitionError(f"Could not acquire promotion lock '{lock_id}' for worker {self.worker_id}.")

        try:
            # 4. Perform Data Promotion (Migration)
            # Find the course in the isolated ULCP DB
            course_doc = ulcp_db.courses.find_one({"slug": course_id}) or ulcp_db.courses.find_one({"_id": course_id})
            if not course_doc:
                # Try finding by job_id link if stored there
                if job and job.get("course_id"):
                    ref_id = job["course_id"]
                    course_doc = ulcp_db.courses.find_one({"slug": ref_id}) or ulcp_db.courses.find_one({"_id": ref_id})
                    
            if not course_doc:
                raise ValueError(f"Course document for {course_id} not found in ULCP database.")

            # Perform data promotion within a MongoDB transaction if supported, else fallback to standard operations
            try:
                with self.platform_db_client.start_session() as session:
                    with session.start_transaction():
                        self._execute_promotion_ops(
                            platform_db=platform_db,
                            course_doc=course_doc,
                            course_id=course_id,
                            job_id=job_id,
                            dedup_id=dedup_id,
                            fingerprint=fingerprint,
                            correlation_id=correlation_id,
                            session=session
                        )
            except Exception as tx_err:
                logger.warning(f"MongoDB transaction failed or is not supported by deployment ({tx_err}). Falling back to non-transactional promotion.")
                self._execute_promotion_ops(
                    platform_db=platform_db,
                    course_doc=course_doc,
                    course_id=course_id,
                    job_id=job_id,
                    dedup_id=dedup_id,
                    fingerprint=fingerprint,
                    correlation_id=correlation_id,
                    session=None
                )

            logger.info(f"Course {course_id} successfully promoted to platform staging.")
            
            # Emit success metrics
            duration_secs = time.time() - start_time
            from src.observability.metrics import metrics
            metrics.emit_metric("PromotionSucceeded")
            metrics.emit_metric("PromotionDuration", value=duration_secs, unit="Seconds")

        except Exception as e:
            # Log failure event
            self._log_promotion_event(
                platform_db=platform_db,
                dedup_id=dedup_id,
                fingerprint=fingerprint,
                job_id=job_id,
                course_id=course_id,
                correlation_id=correlation_id,
                stage="FAILED",
                message=f"Promotion failed: {str(e)}",
                session=None
            )
            raise e
        finally:
            # Release lock
            try:
                lock_manager.release_lock(lock_id, self.worker_id)
            except Exception as e:
                logger.warning(f"Failed to release promotion lock '{lock_id}': {e}")

    def _execute_promotion_ops(
        self,
        platform_db,
        course_doc: Dict[str, Any],
        course_id: str,
        job_id: str,
        dedup_id: str,
        fingerprint: str,
        correlation_id: str,
        session=None
    ) -> None:
        """Executes the sequence of course upsert, idempotency key registration, and success log inside a session/transaction."""
        # Log started event
        self._log_promotion_event(
            platform_db=platform_db,
            dedup_id=dedup_id,
            fingerprint=fingerprint,
            job_id=job_id,
            course_id=course_id,
            correlation_id=correlation_id,
            stage="PROMOTION_STARTED",
            message=f"Beginning course staging promotion (job: {job_id}, dedup_id: {dedup_id}).",
            session=session
        )

        # Upsert into platform courses collection
        dest_col = platform_db.courses
        slug = course_doc.get("slug")
        
        course_to_insert = dict(course_doc)
        if slug:
            dest_col.replace_one({"slug": slug}, course_to_insert, upsert=True, session=session)
        else:
            dest_col.replace_one({"_id": course_doc["_id"]}, course_to_insert, upsert=True, session=session)

        # Write promotion execution record
        execution_doc = {
            "_id": dedup_id,
            "job_id": job_id,
            "course_id": course_id,
            "correlation_id": correlation_id,
            "promoted_at": datetime.datetime.utcnow(),
            "status": "COMPLETED"
        }
        platform_db.promotion_executions.insert_one(execution_doc, session=session)

        # Log success event (immutable log)
        self._log_promotion_event(
            platform_db=platform_db,
            dedup_id=dedup_id,
            fingerprint=fingerprint,
            job_id=job_id,
            course_id=course_id,
            correlation_id=correlation_id,
            stage="PROMOTED",
            message=f"Course successfully promoted to platform staging DB.",
            session=session
        )

    def _log_promotion_event(
        self,
        platform_db,
        dedup_id: str,
        fingerprint: str,
        job_id: str,
        course_id: str,
        correlation_id: str,
        stage: str,
        message: str,
        session=None
    ) -> None:
        """Appends an immutable log event into the platform's promotion_events collection."""
        try:
            task_arn = os.getenv("ECS_TASK_ARN") or "local-task"
            event = {
                "event_id": str(uuid.uuid4()),
                "dedup_id": dedup_id,
                "fingerprint": fingerprint,
                "job_id": job_id,
                "course_id": course_id,
                "correlation_id": correlation_id,
                "trace_id": correlation_id,
                "stage": stage,
                "message": message,
                "worker_id": self.worker_id,
                "task_arn": task_arn,
                "timestamp": datetime.datetime.utcnow()
            }
            if session:
                platform_db.promotion_events.insert_one(event, session=session)
            else:
                platform_db.promotion_events.insert_one(event)
        except Exception as e:
            logger.error(f"Failed to write promotion audit event: {e}")
