import os
import json
import sys
import datetime
from typing import Dict, Any, Optional
from pymongo import MongoClient
import bson
from utils.logger import get_logger
from utils.resilience import retry

logger = get_logger(__name__)

class MongoDBExporter:
    """
    Exports the transformed course document to MongoDB with size validation and retries.
    Handles dynamic program creation and logical deduplication.
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
    def get_or_create_program(self, university_id: str, program_title: str) -> str:
        """
        Finds or creates a program within a university.
        """
        self._ensure_connection()
        programs_col = self._db['programs']
        
        program = programs_col.find_one({
            "universityId": university_id,
            "title": program_title
        })

        if program:
            return str(program["_id"])

        # Create new program if not found
        new_program = {
            "title": program_title,
            "universityId": university_id,
            "created_at": datetime.datetime.utcnow(),
            "status": "active"
        }
        result = programs_col.insert_one(new_program)
        logger.log("INFO", "Dynamic program created", title=program_title, university_id=university_id)
        return str(result.inserted_id)

    def check_logical_duplicate(
        self, 
        university_id: str, 
        program_id: str, 
        title: str, 
        canvas_course_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Checks if a course already exists.
        Prioritizes Canvas ID match, falls back to context + title.
        """
        self._ensure_connection()
        
        # 1. Check by Canvas ID (Strongest Match)
        if canvas_course_id:
            course = self._db['courses'].find_one({
                "universityId": university_id,
                "canvas_course_id": canvas_course_id
            })
            if course:
                return str(course["_id"])
        
        # 2. Check by Title in same Program (Fuzzy Match)
        course = self._db['courses'].find_one({
            "universityId": university_id,
            "programId": program_id,
            "title": title
        })
        return str(course["_id"]) if course else None

    @retry(max_attempts=3, base_delay=1)
    def export(self, course_data: Dict[str, Any]) -> str:
        """
        Inserts the course document into the 'courses' collection with retry and size check.
        """
        self._ensure_connection()
        collection = self._db['courses']

        # 1. Convert string IDs to BSON ObjectIds for Node.js compatibility
        try:
            if "university" in course_data and isinstance(course_data["university"], str):
                course_data["university"] = bson.ObjectId(course_data["university"])
            if "authorId" in course_data and isinstance(course_data["authorId"], str):
                course_data["authorId"] = bson.ObjectId(course_data["authorId"])
            # programId is not in the target JSON but might be passed for legacy/internal use
            if "programId" in course_data and isinstance(course_data["programId"], str):
                course_data["programId"] = bson.ObjectId(course_data["programId"])
        except Exception as e:
            logger.log("WARNING", "Failed to convert IDs to ObjectId", error=str(e))

        # 2. Size Validation
        serialized = bson.BSON.encode(course_data)
        size_bytes = len(serialized)
        
        if size_bytes > self.MAX_BSON_SIZE:
            logger.log("ERROR", "Document exceeds MongoDB size limit", 
                       title=course_data.get('title'), 
                       size_mb=size_bytes/(1024*1024))
            raise ValueError(f"Course document too large ({size_bytes} bytes).")

        # 3. Export (Upsert based on slug to support --force)
        slug = course_data.get('slug')
        if slug:
            result = collection.replace_one(
                {"slug": slug},
                course_data,
                upsert=True
            )
            inserted_id = result.upserted_id or collection.find_one({"slug": slug})["_id"]
        else:
            result = collection.insert_one(course_data)
            inserted_id = result.inserted_id
        
        logger.log("INFO", "Course exported to MongoDB", 
                   course_id=str(inserted_id), 
                   title=course_data.get('title'))
                   
        return str(inserted_id)

    def find_by_checksum(self, checksum: str) -> Optional[Dict[str, Any]]:
        self._ensure_connection()
        return self._db['migration_jobs'].find_one({"package_checksum": checksum})

    def get_job(self, task_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_connection()
        return self._db['migration_jobs'].find_one({"task_id": task_id})

    def create_job(self, task_id: str, s3_key: Optional[str] = None):
        self._ensure_connection()
        job = {
            "task_id": task_id,
            "s3_key": s3_key,
            "status": "pending",
            "progress": 0,
            "logs": ["Job initialized"],
            "startedAt": datetime.datetime.utcnow(),
            "updatedAt": datetime.datetime.utcnow()
        }
        self._db['migration_jobs'].insert_one(job)

    def update_job_status(self, task_id: str, status: str, log_msg: str = None, progress: int = None):
        self._ensure_connection()
        update_doc = {
            "status": status,
            "updatedAt": datetime.datetime.utcnow()
        }
        if progress is not None:
            update_doc["progress"] = progress
        
        query = {"task_id": task_id}
        if log_msg:
            self._db['migration_jobs'].update_one(query, {"$push": {"logs": log_msg}})
        
        if status == "completed":
            update_doc["completedAt"] = datetime.datetime.utcnow()
            
        self._db['migration_jobs'].update_one(query, {"$set": update_doc})

    def track_job(self, task_id: str, checksum: str, status: str, course_id: str = None):
        """Legacy support for checksum-based tracking."""
        self._ensure_connection()
        self._db['migration_jobs'].update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "package_checksum": checksum,
                    "status": status,
                    "course_id": course_id,
                    "updatedAt": datetime.datetime.utcnow()
                },
                "$setOnInsert": {"startedAt": datetime.datetime.utcnow()}
            },
            upsert=True
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
