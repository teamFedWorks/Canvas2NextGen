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

def to_object_id(value: Any, field_name: str) -> bson.ObjectId:
    raw_value = str(value or "").strip()
    if not raw_value or not bson.ObjectId.is_valid(raw_value):
        raise ValueError(f"{field_name} must be a valid MongoDB ObjectId. Received: {value!r}")
    return bson.ObjectId(raw_value)

class MongoDBExporter:
    """
    Exports the transformed course document to MongoDB with size validation and retries.
    Handles dynamic program creation and logical deduplication.
    """

    MAX_BSON_SIZE = 15.5 * 1024 * 1024  # 15.5MB (safe margin below 16MB)

    def __init__(self, mongodb_uri: str, database_name: str = "test"):
        if not mongodb_uri:
            raise ValueError("MongoDB connection URI must be explicitly provided to MongoDBExporter.")
        self.uri = mongodb_uri
        self.db_name = database_name or "test"
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

        import re
        bundle_url = re.sub(r'[-\s]+', '-', re.sub(r'[^\w\s-]', '', program_title.lower())).strip('-')

        # Also check by bundleUrl to prevent unique index duplicates
        program = programs_col.find_one({
            "universityId": university_id,
            "bundleUrl": bundle_url
        })
        if program:
            return str(program["_id"])

        # Create new program if not found
        new_program = {
            "title": program_title,
            "bundleUrl": bundle_url,
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
                "createdBy": bson.ObjectId(university_id),
                "canvas_course_id": canvas_course_id
            })
            if course:
                return str(course["_id"])
        
        # 2. Check by Title in same Program (Fuzzy Match)
        course = self._db['courses'].find_one({
            "createdBy": bson.ObjectId(university_id),
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

        # 1. Convert and validate IDs before writing. Empty strings are never safe
        # for ObjectId reference fields because Node/Mongoose populate() will cast
        # and crash on them later.
        course_data["university"] = to_object_id(course_data.get("university"), "course.university")
        course_data["authorId"] = to_object_id(course_data.get("authorId"), "course.authorId")

        # Generate missing IDs for curriculum modules and items to prevent navigation rendering issue
        curriculum = course_data.get("curriculum", [])
        for mod in curriculum:
            if "_id" not in mod or mod["_id"] is None:
                mod["_id"] = bson.ObjectId()
            items = mod.get("items", [])
            for item in items:
                if "_id" not in item or item["_id"] is None:
                    item["_id"] = bson.ObjectId()

        # ── Onboarding schema enforcement ──────────────────────────────────────
        # university  → ""          (blank; platform populates at publish time)
        # programId   → ""          (blank; platform populates at publish time)
        # createdBy   → university._id  (who initiated the course creation)
        # status      → "draft"
        course_data["createdBy"] = course_data["university"]
        course_data["universityId"] = str(course_data["university"])
        course_data["programId"]  = None
        course_data["status"]     = "Draft"

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
            query = {"slug": slug, "university": course_data["university"]}
            result = collection.replace_one(
                query,
                course_data,
                upsert=True
            )
            inserted_id = result.upserted_id or collection.find_one(query)["_id"]
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
