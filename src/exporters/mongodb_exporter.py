import os
import json
import sys
from typing import Dict, Any
from pymongo import MongoClient
import bson
from ..utils.logger import get_logger
from ..utils.resilience import retry

logger = get_logger(__name__)

class MongoDBExporter:
    """
    Exports the transformed course document to MongoDB with size validation and retries.
    """

    MAX_BSON_SIZE = 15.5 * 1024 * 1024  # 15.5MB (safe margin below 16MB)

    def __init__(self, mongodb_uri: str = None, database_name: str = None):
        self.uri = mongodb_uri or os.getenv("MONGODB_URI")
        self.db_name = database_name or os.getenv("MONGODB_DATABASE", "lms_db")
        self._client = None
        self._db = None

    def _ensure_connection(self):
        if not self._client:
            self._client = MongoClient(self.uri)
            self._db = self._client[self.db_name]

    @retry(max_attempts=3, base_delay=1)
    def export(self, course_data: Dict[str, Any]) -> str:
        """
        Inserts the course document into the 'courses' collection with retry and size check.
        """
        self._ensure_connection()
        collection = self._db['courses']

        # 1. Size Validation
        # Estimates BSON size before inserting
        serialized = bson.BSON.encode(course_data)
        size_bytes = len(serialized)
        
        logger.log("INFO", "Validating document size", 
                   title=course_data.get('title'), 
                   size_bytes=size_bytes)

        if size_bytes > self.MAX_BSON_SIZE:
            logger.log("ERROR", "Document exceeds MongoDB size limit", 
                       title=course_data.get('title'), 
                       size_mb=size_bytes/(1024*1024))
            raise ValueError(f"Course document too large ({size_bytes} bytes). Ingestion aborted.")

        # 2. Export
        result = collection.insert_one(course_data)
        
        logger.log("INFO", "Course exported to MongoDB", 
                   course_id=str(result.inserted_id), 
                   title=course_data.get('title'))
                   
        return str(result.inserted_id)

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def find_by_checksum(self, checksum: str) -> Optional[Dict[str, Any]]:
        """
        Used for idempotency check.
        """
        self._ensure_connection()
        return self._db['migration_jobs'].find_one({"package_checksum": checksum})

    def track_job(self, task_id: str, checksum: str, status: str, course_id: str = None):
        """
        Tracks migration jobs for idempotency and status monitoring.
        """
        self._ensure_connection()
        self._db['migration_jobs'].update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "package_checksum": checksum,
                    "status": status,
                    "course_id": course_id,
                    "updated_at": bson.datetime.datetime.utcnow()
                },
                "$setOnInsert": {"created_at": bson.datetime.datetime.utcnow()}
            },
            upsert=True
        )
