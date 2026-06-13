import boto3
import json
import logging
import time
from typing import Dict, Any, Optional
from pathlib import Path

from botocore.exceptions import ClientError
from src.ucae.workflow.input_source import S3InputSource
from src.ucae.workflow.extraction import ExtractionService
from src.ucae.providers.registry import ProviderRegistry
from src.ucae.workflow.context import PipelineContext
from src.ucae.workflow.jobs import JobPersistenceService
from src.ucae.workflow.state import JobState
from src.ucae.canonical.normalizer import CanonicalNormalizer
from src.ucae.workflow.exceptions import QuarantineError, LockAcquisitionError, DeadLetterError
from src.ucae.workflow.lock import MongoLockManager, LockHeartbeat

from src.observability.tracing import TraceLogger
logger = TraceLogger(__name__)


class IngestionQueueListener:
    """
    IngestionQueueListener polls SQS for course package events (routed via EventBridge).
    Manages SQS visibility timeouts, updates JobContext, and runs the ingestion workflow.
    """
    def __init__(
        self,
        queue_url: str,
        db_client,
        provider_registry: ProviderRegistry,
        normalizer: CanonicalNormalizer,
        s3_client=None,
        sqs_client=None,
        visibility_timeout_secs: int = 300,
        worker_id: str = "worker_1",
        temp_base_dir: Optional[Path] = None
    ):
        self.queue_url = queue_url
        self.db_client = db_client
        self.provider_registry = provider_registry
        self.normalizer = normalizer
        self.s3_client = s3_client or boto3.client("s3")
        self.sqs_client = sqs_client or boto3.client("sqs")
        self.visibility_timeout_secs = visibility_timeout_secs
        self.worker_id = worker_id
        
        self.persistence = JobPersistenceService(db_client)
        self.extraction_service = ExtractionService(temp_base_dir=temp_base_dir)

    def register_worker_heartbeat(self) -> None:
        """Registers this worker task in the worker_heartbeats collection."""
        try:
            import os
            import socket
            import datetime
            db = self.db_client.get_database()
            hostname = socket.gethostname()
            
            worker_ver = os.getenv("WORKER_VERSION") or "2026.06.13"
            task_arn = os.getenv("ECS_TASK_ARN") or "local-task"
            
            doc = {
                "_id": self.worker_id,
                "workerId": self.worker_id,
                "taskArn": task_arn,
                "hostname": hostname,
                "version": worker_ver,
                "startedAt": datetime.datetime.utcnow(),
                "lastHeartbeat": datetime.datetime.utcnow()
            }
            db.worker_heartbeats.replace_one({"_id": self.worker_id}, doc, upsert=True)
            logger.info(f"Worker heartbeat registered in MongoDB for {self.worker_id}")
        except Exception as e:
            logger.warning(f"Failed to register worker heartbeat: {e}")

    def run_startup_self_checks(self, intake_bucket: str, artifact_bucket: str) -> None:
        """
        Runs startup validation checks to verify credentials and permissions for S3, SQS, and MongoDB.
        Raises RuntimeError if critical permission checks fail. Logs warnings for non-critical warnings like index issues.
        """
        logger.info("Running worker startup self-checks...")
        
        status_sts = "FAIL"
        status_mongo = "FAIL"
        status_indexes = "FAIL"
        status_sqs = "FAIL"
        status_s3_intake = "FAIL"
        status_s3_artifact = "FAIL"

        # 1. AWS STS Caller Identity Check
        try:
            sts_client = boto3.client("sts")
            identity = sts_client.get_caller_identity()
            logger.info(f"  ✓ AWS STS Identity verified: {identity.get('Arn')}")
            status_sts = "PASS"
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: AWS credentials/STS check error: {e}")

        # 2. MongoDB connectivity & required indexes validation
        try:
            db = self.db_client.get_database()
            self.db_client.admin.command('ping')
            logger.info("  ✓ MongoDB connectivity verified.")
            status_mongo = "PASS"
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: MongoDB connectivity error: {e}")

        # Verify required indexes exist and raise error if missing
        try:
            # 1. jobs: unique index on job_id
            jobs_info = db.jobs.index_information()
            has_job_id = any(
                info.get('key') == [('job_id', 1)] and info.get('unique', False)
                for info in jobs_info.values()
            )
            if not has_job_id:
                raise RuntimeError("Required unique index missing: jobs.job_id.")

            # 2. locks: TTL index on expires_at
            locks_info = db.locks.index_information()
            has_locks_ttl = any(
                info.get('key') == [('expires_at', 1)] and 'expireAfterSeconds' in info
                for info in locks_info.values()
            )
            if not has_locks_ttl:
                raise RuntimeError("Required TTL index missing: locks.expires_at.")

            # 3. assets: TTL index on expiresAt
            assets_info = db.assets.index_information()
            has_assets_ttl = any(
                info.get('key') == [('expiresAt', 1)] and 'expireAfterSeconds' in info
                for info in assets_info.values()
            )
            if not has_assets_ttl:
                raise RuntimeError("Required TTL index missing: assets.expiresAt.")

            # 4. worker_heartbeats: TTL index on lastHeartbeat
            hb_info = db.worker_heartbeats.index_information()
            has_hb_ttl = any(
                info.get('key') == [('lastHeartbeat', 1)] and 'expireAfterSeconds' in info
                for info in hb_info.values()
            )
            if not has_hb_ttl:
                raise RuntimeError("Required TTL index missing: worker_heartbeats.lastHeartbeat.")

            logger.info("  ✓ MongoDB required indexes verified.")
            status_indexes = "PASS"
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: MongoDB index verification error: {e}")

        # 3. SQS Permissions Check (GetQueueAttributes only)
        try:
            self.sqs_client.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=['QueueArn'])
            logger.info("  ✓ SQS queue access and GetQueueAttributes verified.")
            status_sqs = "PASS"
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ['AccessDenied', 'AccessDeniedException']:
                raise RuntimeError("Worker startup failed: Missing permission sqs:GetQueueAttributes")
            raise RuntimeError(f"Worker startup failed: SQS queue access error: {e}")
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: SQS check error: {e}")

        # 4. S3 Intake Bucket Readable Check
        try:
            self.s3_client.head_bucket(Bucket=intake_bucket)
            logger.info(f"  ✓ S3 intake bucket '{intake_bucket}' read access verified.")
            status_s3_intake = "PASS"
        except ClientError as e:
            raise RuntimeError(f"Worker startup failed: Missing permission s3:GetObject or s3:ListBucket on {intake_bucket}. Details: {e}")
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: S3 intake check error: {e}")

        # 5. S3 Artifact Bucket Readable Check (Read-Only startup: HeadBucket + GetBucketLocation)
        try:
            self.s3_client.head_bucket(Bucket=artifact_bucket)
            self.s3_client.get_bucket_location(Bucket=artifact_bucket)
            logger.info(f"  ✓ S3 artifact bucket '{artifact_bucket}' head and location access verified.")
            status_s3_artifact = "PASS"
        except ClientError as e:
            raise RuntimeError(f"Worker startup failed: Missing permission s3:ListBucket or s3:GetBucketLocation on {artifact_bucket}. Details: {e}")
        except Exception as e:
            raise RuntimeError(f"Worker startup failed: S3 artifact read check error: {e}")

        # Emit single structured log
        import os
        status_s3 = "PASS" if status_s3_intake == "PASS" and status_s3_artifact == "PASS" else "FAIL"
        env_val = os.getenv("ENVIRONMENT") or os.getenv("NODE_ENV") or "staging"
        worker_ver = os.getenv("WORKER_VERSION") or "2026.06.13"
        git_sha = os.getenv("GIT_SHA") or "unknown"
        img_tag = os.getenv("IMAGE_TAG") or "v1.0.3"
        cluster_val = os.getenv("CLUSTER_NAME") or "nextgen-lms-ecs-staging"

        structured_log = {
            "event": "startup",
            "environment": env_val,
            "workerVersion": worker_ver,
            "gitSha": git_sha,
            "imageTag": img_tag,
            "cluster": cluster_val,
            "mongo": status_mongo,
            "s3": status_s3,
            "sqs": status_sqs,
            "indexes": status_indexes
        }
        logger.info(f"Startup check results: {json.dumps(structured_log)}")

        # 6. Register Worker Heartbeat
        self.register_worker_heartbeat()

        logger.info("Worker startup self-checks passed successfully.")

    def poll_messages(self, max_messages: int = 1, wait_time_seconds: int = 10) -> None:
        """
        Polls a single batch of SQS messages and processes them.
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                AttributeNames=["ApproximateReceiveCount"]
            )
        except Exception as e:
            logger.error(f"Error receiving messages from SQS: {e}")
            return

        messages = response.get("Messages", [])
        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1))
            
            try:
                # Process the message
                self._process_message(message, receipt_handle, receive_count)
                
                # Delete successfully processed message from queue
                self.sqs_client.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
            except QuarantineError as e:
                logger.error(f"Quarantine error for message {message['MessageId']}: {e}")
                from src.observability.metrics import metrics
                metrics.emit_metric("QuarantineTriggered")
                metrics.emit_metric("JobsFailed")
                try:
                    self.sqs_client.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle
                    )
                except Exception as del_err:
                    logger.warning(f"Failed to delete quarantined SQS message: {del_err}")
            except LockAcquisitionError as e:
                logger.warning(f"Lock acquisition failed for message {message['MessageId']}: {e}. Leaving message in SQS.")
                from src.observability.metrics import metrics
                metrics.emit_metric("LockAcquisitionFailed")
            except Exception as e:
                logger.exception(f"Failed to process SQS message: {message['MessageId']}")
                from src.observability.metrics import metrics
                metrics.emit_metric("JobsFailed")
                # Shorten visibility timeout on failure so it can be retried or routed to DLQ quickly
                try:
                    self.sqs_client.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=10
                    )
                except Exception:
                    pass

    def _process_message(self, message: Dict[str, Any], receipt_handle: str, receive_count: int) -> None:
        """Parses the message, checks idempotency, and runs the workflow stages."""
        import time
        start_time = time.time()
        from src.observability.metrics import metrics
        metrics.emit_metric("JobsStarted")
        body = json.loads(message["Body"])
        
        # Check if S3 EventBridge payload
        detail = body.get("detail", {})
        bucket = detail.get("bucket", {}).get("name")
        key = detail.get("object", {}).get("key")
        version_id = detail.get("object", {}).get("version-id")
        
        # Fallback to direct payload
        if not bucket or not key:
            bucket = body.get("bucket")
            key = body.get("key")
            version_id = body.get("version_id")

        if not bucket or not key:
            raise ValueError(f"Invalid message format, bucket/key missing: {body}")

        # Derive a stable job ID
        job_id = body.get("job_id") or f"job_{hash(f'{bucket}/{key}/{version_id}') & 0xffffffff:08x}"
        
        # Start Trace Context
        correlation_id = body.get("correlation_id") or body.get("correlationId")
        from src.observability.tracing import TracingMiddleware
        trace = TracingMiddleware.job_trace(job_id, correlation_id)
        
        # 1. Acquire Lock
        lock_manager = MongoLockManager(self.db_client)
        if not lock_manager.acquire_lock(lock_id=job_id, worker_id=self.worker_id, lease_secs=60):
            # End Trace Context on failure
            import datetime
            trace.completed_at = datetime.datetime.utcnow()
            from src.observability.tracing import _correlation_id_ctx, _trace_ctx
            _correlation_id_ctx.set(None)
            _trace_ctx.set(None)
            raise LockAcquisitionError(f"Could not acquire lock for job {job_id}.")

        # Start Lock Heartbeat
        heartbeat = LockHeartbeat(self.db_client, lock_id=job_id, worker_id=self.worker_id, lease_secs=60)
        heartbeat.start()

        try:
            # Initialize Job record in DB
            self.persistence.create_job(job_id, payload_metadata={
                "bucket": bucket,
                "key": key,
                "version_id": version_id,
                "receive_count": receive_count,
                "sqs_message_id": message["MessageId"],
                "correlation_id": trace.correlation_id
            })

            # Idempotency Check: if job already succeeded, skip it
            db = self.db_client.get_database()
            existing_job = db.jobs.find_one({"job_id": job_id})
            if existing_job and existing_job.get("status") == JobState.SUCCESS.value:
                logger.info(f"Job {job_id} already completed successfully. Skipping.")
                return

            # Record retries
            if receive_count > 1:
                self.persistence.log_event(
                    job_id=job_id,
                    stage="RETRY_STARTED",
                    message=f"Processing retry attempt #{receive_count-1}.",
                    details={"receive_count": receive_count}
                )

            # Extend SQS visibility timeout to start the job
            self._extend_visibility(receipt_handle)

            # Set up PipelineContext
            context = PipelineContext(job_id=job_id, logger=logger, persistence=self.persistence)
            context.add_event(JobState.DOWNLOAD_STARTED.value, f"Starting download from S3: {bucket}/{key}")

            try:
                # 1. Download & Extract
                input_source = S3InputSource(
                    bucket=bucket,
                    key=key,
                    version_id=version_id,
                    s3_client=self.s3_client
                )
                
                context.add_event(JobState.EXTRACTION_STARTED.value, "Extracting course package...")
                workspace = self.extraction_service.prepare(input_source)
                context.workspace = workspace
                
                # Extend SQS visibility after extraction to keep processing alive
                self._extend_visibility(receipt_handle)
                context.add_event(JobState.EXTRACTION_FINISHED.value, "Package extracted safely.")

                # 2. Detect Provider
                candidates = self.provider_registry.detect_provider(workspace)
                matched_candidate = next((c for c in candidates if c.result.matched), None)
                
                if not matched_candidate:
                    raise ValueError("No matching LMS provider resolved for package format.")

                provider = matched_candidate.provider
                detected_version = matched_candidate.result.detected_version
                context.provider_metadata = provider.metadata
                context.add_event(
                    JobState.DETECTED.value,
                    f"Format detected: {provider.metadata.name} (Version: {detected_version})",
                    metadata={"provider_id": provider.metadata.id, "confidence": matched_candidate.result.confidence}
                )

                # 3. Parse Provider Model
                context.add_event(JobState.PARSE_STARTED.value, f"Parsing package contents using {provider.metadata.name} parser...")
                provider_model = provider.parse(workspace)
                
                # Save parsed model reference on disk
                context.save_provider_model(provider_model)
                context.add_event(JobState.PARSE_FINISHED.value, "Package parsing completed.")

                # 4. Validate Source
                context.add_event(JobState.VALIDATION_STARTED.value, "Running source validation...")
                issues = provider.validate_source(provider_model)
                context.validation_issues.extend(issues)
                
                # Filter error issues that block ingestion
                errors = [i for i in issues if i.severity == "error"]
                if errors:
                    raise ValueError(f"Source validation failed with {len(errors)} error(s).")
                
                context.add_event(JobState.VALIDATION_FINISHED.value, "Source validation checks passed.")

                # 5. Build Canonical Course
                context.add_event("CANONICAL_BUILD_STARTED", "Building canonical curriculum mapping...")
                canonical_course = provider.build_canonical(provider_model)
                context.save_canonical_course(canonical_course, is_normalized=False)
                context.add_event(JobState.PARSE_FINISHED.value, "Canonical course mapping built.")

                # 6. Normalize Canonical Course
                normalized_course = self.normalizer.normalize(canonical_course)
                context.save_canonical_course(normalized_course, is_normalized=True)
                
                fingerprint = self.normalizer.compute_content_fingerprint(normalized_course)
                context.add_metric("content_fingerprint", fingerprint)
                db.jobs.update_one({"job_id": job_id}, {"$set": {"content_fingerprint": fingerprint}})
                context.add_event("NORMALIZATION_FINISHED", f"Canonical normalization completed. Fingerprint: {fingerprint}")

                # Save context references
                self.persistence.save_context_references(job_id, context)
                # Update persistence status to success
                self.persistence.update_job_status(job_id, JobState.SUCCESS)
                context.add_event(JobState.SUCCESS.value, "Course ingestion pipeline completed successfully.")
                
                # Emit success and duration metrics
                duration_secs = time.time() - start_time
                from src.observability.metrics import metrics
                metrics.emit_metric("JobsSucceeded")
                metrics.emit_metric("JobDuration", value=duration_secs, unit="Seconds")

            except QuarantineError as e:
                self.persistence.save_context_references(job_id, context)
                self.persistence.update_job_status(job_id, JobState.QUARANTINED, error_message=str(e))
                raise
            except Exception as e:
                # Save context references even on failure so we know how far it got
                self.persistence.save_context_references(job_id, context)
                # Update persistence status to failed
                self.persistence.update_job_status(job_id, JobState.FAILED, error_message=str(e))
                raise
        finally:
            heartbeat.stop()
            heartbeat.join(timeout=5)
            try:
                lock_manager.release_lock(job_id, self.worker_id)
            except Exception as e:
                logger.warning(f"Failed to release lock for job {job_id}: {e}")
            
            # End Trace Context
            import datetime
            trace.completed_at = datetime.datetime.utcnow()
            from src.observability.tracing import _correlation_id_ctx, _trace_ctx
            _correlation_id_ctx.set(None)
            _trace_ctx.set(None)

    def _extend_visibility(self, receipt_handle: str) -> None:
        """Extends SQS message visibility timeout to prevent concurrent execution."""
        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=self.visibility_timeout_secs
            )
        except Exception as e:
            logger.warning(f"Failed to extend SQS visibility: {e}")
