"""
SQS Event-Driven Ingestion Queue

Provides decoupled, scalable ingestion via AWS SQS.
Separates API reception from actual processing.

Usage:
    # Producer (API)
    sqs.enqueue_job(job_id, s3_key, university_id)
    
    # Consumers (Workers)
    sqc = SQSJobConsumer()
    for message in sqc.consume():
        job = message.parse()
        process(job)
        sqc.acknowledge(message)
"""

import os
import json
import uuid
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from core.orchestrator import JobOrchestrator, IngestionJob, JobState
from core.classifier import classify_source, ClassificationResult
from core.canonical_pipeline import CanonicalPipeline
from observability.logger import get_logger
from services.canonical_migration_service import CanonicalMigrationService

logger = get_logger(__name__)


@dataclass
class JobMessage:
    """SQS message payload for an ingestion job."""
    job_id: str
    correlation_id: str
    source_type: str  # "s3", "upload", "canvas_api"
    source_path: str  # s3_key or file_path
    university_id: Optional[str] = None
    author_id: Optional[str] = None
    course_code: Optional[str] = None
    force: bool = False
    priority: int = 1  # 1=normal, 2=high, 3=urgent
    
    def to_sqs_message(self) -> Dict[str, Any]:
        """Serialize to SQS message body."""
        return {
            "job_id": self.job_id,
            "correlation_id": self.correlation_id,
            "source_type": self.source_type,
            "source_path": self.source_path,
            "university_id": self.university_id,
            "author_id": self.author_id,
            "course_code": self.course_code,
            "force": self.force,
            "priority": self.priority,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    @classmethod
    def from_sqs_message(cls, message: Dict[str, Any]) -> 'JobMessage':
        """Deserialize from SQS message."""
        body = json.loads(message['Body'])
        return cls(
            job_id=body["job_id"],
            correlation_id=body.get("correlation_id", str(uuid.uuid4())),
            source_type=body["source_type"],
            source_path=body["source_path"],
            university_id=body.get("university_id"),
            author_id=body.get("author_id"),
            course_code=body.get("course_code"),
            force=body.get("force", False),
            priority=body.get("priority", 1),
        )


class SQSJobQueue:
    """
    Manages SQS queues for ingestion jobs.
    
    Queues:
    - ingestion_jobs: Main queue for job requests
    - ingestion_dlq: Dead letter queue for failed jobs (>3 retries)
    """
    
    def __init__(
        self,
        queue_url: Optional[str] = None,
        dlq_url: Optional[str] = None,
        region: str = "us-east-2"
    ):
        self.queue_url = queue_url or os.getenv("SQS_QUEUE_URL", "")
        self.dlq_url = dlq_url or os.getenv("SQS_DLQ_URL", "")
        
        self.sqs = boto3.client('sqs', region_name=region)
        
        # Visibility timeout: how long job is hidden from other workers
        self.visibility_timeout = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "300"))  # 5 minutes
        
        # Wait time for long polling
        self.wait_time = int(os.getenv("SQS_WAIT_TIME", "20"))  # 20 seconds
    
    def enqueue_job(
        self,
        job_id: str,
        source_type: str,
        source_path: str,
        university_id: Optional[str] = None,
        author_id: Optional[str] = None,
        course_code: Optional[str] = None,
        force: bool = False,
        priority: int = 1,
        delay_seconds: int = 0
    ) -> bool:
        """
        Enqueue a new ingestion job.
        
        Returns:
            True if enqueued successfully
        """
        message = JobMessage(
            job_id=job_id,
            correlation_id=str(uuid.uuid4()),
            source_type=source_type,
            source_path=source_path,
            university_id=university_id,
            author_id=author_id,
            course_code=course_code,
            force=force,
            priority=priority,
        )
        
        try:
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message.to_sqs_message()),
                DelaySeconds=delay_seconds,
                MessageAttributes={
                    'SourceType': {
                        'StringValue': source_type,
                        'DataType': 'String'
                    },
                    'Priority': {
                        'StringValue': str(priority),
                        'DataType': 'Number'
                    }
                }
            )
            
            logger.info("Job enqueued",
                       extra={
                           "job_id": job_id,
                           "message_id": response.get("MessageId"),
                           "queue": self.queue_url
                       })
            return True
            
        except ClientError as e:
            logger.error("Failed to enqueue job", extra={"error": str(e), "job_id": job_id})
            return False
    
    def receive_jobs(
        self,
        max_messages: int = 10,
        wait_time: Optional[int] = None
    ) -> list:
        """
        Poll queue for jobs.
        
        Returns:
            List of (message_body, receipt_handle) tuples
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time or self.wait_time,
                VisibilityTimeout=self.visibility_timeout,
                MessageAttributeNames=['All']
            )
            
            messages = []
            for msg in response.get('Messages', []):
                body = json.loads(msg['Body'])
                messages.append({
                    "job_id": body["job_id"],
                    "correlation_id": body.get("correlation_id"),
                    "source_type": body["source_type"],
                    "source_path": body["source_path"],
                    "university_id": body.get("university_id"),
                    "author_id": body.get("author_id"),
                    "course_code": body.get("course_code"),
                    "force": body.get("force", False),
                    "receipt_handle": msg['ReceiptHandle'],
                    "message_id": msg.get('MessageId'),
                })
            
            return messages
            
        except ClientError as e:
            logger.error("Failed to receive messages", extra={"error": str(e)})
            return []
    
    def acknowledge_job(self, receipt_handle: str, job_id: str) -> bool:
        """
        Delete processed message from queue.
        
        This signals successful completion.
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug("Job acknowledged", extra={"job_id": job_id})
            return True
        except ClientError as e:
            logger.error("Failed to acknowledge job", extra={"error": str(e), "job_id": job_id})
            return False
    
    def send_to_dlq(self, job_data: Dict[str, Any], error: str) -> bool:
        """
        Move failed job to dead letter queue.
        
        Used after max retries exhausted.
        """
        try:
            body = {
                **job_data,
                "failed_at": datetime.utcnow().isoformat(),
                "error": error,
                "retry_reason": "max_attempts_exceeded"
            }
            
            self.sqs.send_message(
                QueueUrl=self.dlq_url,
                MessageBody=json.dumps(body)
            )
            
            logger.warning("Job sent to DLQ", extra={"job_id": job_data["job_id"], "error": error})
            return True
        except ClientError as e:
            logger.error("Failed to send to DLQ", extra={"error": str(e)})
            return False


class SQSJobConsumer:
    """
    Worker that consumes jobs from SQS and processes them.
    
    This replaces the API's BackgroundTasks with a scalable worker pool.
    
    Usage:
        consumer = SQSJobConsumer()
        consumer.run_forever()
    """
    
    def __init__(
        self,
        queue_url: Optional[str] = None,
        max_workers: int = 10,
        region: str = "us-east-2"
    ):
        self.queue_url = queue_url or os.getenv("SQS_QUEUE_URL", "")
        self.max_workers = max_workers
        self.sqs = SQSJobQueue(queue_url, region=region)
        self.service = CanonicalMigrationService()
        self.running = False
    
    def process_job(self, job_data: Dict[str, Any]) -> bool:
        """
        Process a single ingestion job.
        
        Returns:
            True if successful, False otherwise
        """
        job_id = job_data["job_id"]
        source_type = job_data["source_type"]
        source_path = job_data["source_path"]
        
        logger.info("Processing job", extra={
            "job_id": job_id,
            "source_type": source_type,
            "source_path": source_path
        })
        
        try:
            # Create job in orchestrator
            from core.orchestrator import JobOrchestrator
            orchestrator = JobOrchestrator()
            job = orchestrator.create_job(
                job_id=job_id,
                source_type=source_type,
                source_path=source_path,
                correlation_id=job_data.get("correlation_id")
            )
            
            # Execute based on source type
            if source_type == "s3":
                # Download from S3 first
                from utils.s3_utils import S3Downloader
                downloader = S3Downloader()
                zip_path = self.service.uploads_dir / f"{job_id}.zip"
                
                if not downloader.download(source_path, zip_path):
                    raise Exception(f"Failed to download from S3: {source_path}")
                
                # Process migration
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(
                        self.service.process_migration(
                            task_id=job_id,
                            file=None,
                            university_id=job_data.get("university_id"),
                            author_id=job_data.get("author_id"),
                            course_code=job_data.get("course_code"),
                            file_path=zip_path,
                            correlation_id=job_data.get("correlation_id"),
                            force=job_data.get("force", False)
                        )
                    )
                finally:
                    loop.close()
                
                # Cleanup
                if zip_path.exists():
                    zip_path.unlink()
                    
            elif source_type == "canvas":
                # Direct Canvas API ingestion
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(
                        self.service.process_migration_from_s3(
                            task_id=job_id,
                            s3_key=job_data.get("s3_key", ""),
                            university_id=job_data.get("university_id"),
                            author_id=job_data.get("author_id"),
                            course_code=job_data.get("course_code")
                        )
                    )
                finally:
                    loop.close()
            else:
                logger.error(f"Unknown source type: {source_type}", extra={"job_id": job_id})
                return False
            
            # Success
            logger.info("Job completed successfully", extra={"job_id": job_id})
            return True
            
        except Exception as e:
            logger.error("Job processing failed", extra={"job_id": job_id, "error": str(e)})
            return False
    
    def run_forever(self):
        """
        Main worker loop.
        
        Continuously poll SQS, process jobs, acknowledge success.
        """
        self.running = True
        logger.info("SQS worker started", extra={"max_workers": self.max_workers})
        
        while self.running:
            try:
                # Poll for jobs
                jobs = self.sqs.receive_jobs(max_messages=self.max_workers)
                
                if not jobs:
                    continue
                
                logger.info(f"Processing {len(jobs)} jobs")
                
                # Process each job
                for job_data in jobs:
                    job_id = job_data["job_id"]
                    receipt_handle = job_data["receipt_handle"]
                    
                    try:
                        success = self.process_job(job_data)
                        
                        if success:
                            self.sqs.acknowledge_job(receipt_handle, job_id)
                        else:
                            # Processing failed - send to DLQ
                            self.sqs.send_to_dlq(job_data, "Processing failed")
                            self.sqs.acknowledge_job(receipt_handle, job_id)
                            
                    except Exception as e:
                        logger.exception("Job processing crashed")
                        # Send to DLQ
                        self.sqs.send_to_dlq(job_data, str(e))
                        # Still acknowledge to remove from main queue
                        self.sqs.acknowledge_job(receipt_handle, job_id)
                
            except KeyboardInterrupt:
                logger.info("Worker shutting down")
                self.running = False
                break
            except Exception as e:
                logger.error("Worker loop error", extra={"error": str(e)})
                # Brief pause before retrying
                import time
                time.sleep(5)
        
        logger.info("Worker stopped")


def create_queues(
    queue_name: str = "ingestion_jobs",
    dlq_name: str = "ingestion_dlq",
    region: str = "us-east-2"
) -> tuple[str, str]:
    """
    Create SQS queues (main + dead letter) if they don't exist.
    
    Returns:
        (queue_url, dlq_url)
    """
    sqs = boto3.client('sqs', region_name=region)
    
    # Create DLQ first (required for redrive policy)
    try:
        dlq_response = sqs.create_queue(
            QueueName=dlq_name,
            Attributes={
                'MessageRetentionPeriod': '1209600',  # 14 days
            }
        )
        dlq_url = dlq_response['QueueUrl']
        logger.info(f"Created DLQ: {dlq_url}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'QueueAlreadyExists':
            dlq_response = sqs.get_queue_url(QueueName=dlq_name)
            dlq_url = dlq_response['QueueUrl']
        else:
            raise
    
    # Create main queue with redrive policy
    try:
        queue_response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'VisibilityTimeout': str(300),
                'MessageRetentionPeriod': '1209600',
                'RedrivePolicy': json.dumps({
                    'deadLetterTargetArn': sqs.get_queue_attributes(
                        QueueUrl=dlq_url,
                        AttributeNames=['QueueArn']
                    )['Attributes']['QueueArn'],
                    'maxReceiveCount': '3'
                })
            }
        )
        queue_url = queue_response['QueueUrl']
        logger.info(f"Created queue: {queue_url}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'QueueAlreadyExists':
            queue_response = sqs.get_queue_url(QueueName=queue_name)
            queue_url = queue_response['QueueUrl']
        else:
            raise
    
    return queue_url, dlq_url