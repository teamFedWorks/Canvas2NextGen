from typing import Dict, List, Any, Optional
from datetime import datetime
import pymongo
from pymongo import ASCENDING
import bson
from bson import ObjectId

from ..models.lms_models import LmsCourse, LmsModule, LmsLesson, LmsQuiz, LmsAssignment
from ..config.mongodb_config import MongoDBConfig
from ..observability.logger import get_logger
from ..utils.resilience import retry

logger = get_logger(__name__)


class MongoDBUploader:
    """
    Directly writes LMS course data to the MongoDB 'courses' collection.
    Matches the nested schema expected by CourseModel.js.
    """

    MAX_BSON_SIZE = 15.5 * 1024 * 1024  # 15.5MB (safe margin below 16MB)

    def __init__(self, config: Optional[MongoDBConfig] = None):
        """
        Initialize connection to MongoDB using external configuration.
        """
        self.config = config or MongoDBConfig()
        self._client: Optional[pymongo.MongoClient] = None
        self._db: Optional[pymongo.database.Database] = None
        self._col_courses: Optional[pymongo.collection.Collection] = None
        self._col_modules: Optional[pymongo.collection.Collection] = None
        self._col_items: Optional[pymongo.collection.Collection] = None
        self._col_jobs: Optional[pymongo.collection.Collection] = None

    def _ensure_connection(self):
        """Lazy connection initialization for better resource management in multiprocessing."""
        if not self._client:
            self._client = pymongo.MongoClient(self.config.mongodb_uri)
            self._db = self._client[self.config.database_name]
            self._col_courses = self._db['courses']
            self._col_modules = self._db['modules']
            self._col_items = self._db['course_items']
            self._col_jobs = self._db['migration_jobs']
            self._ensure_indexes()

    def _ensure_indexes(self):
        """Build essential indexes for normalized collections."""
        if self._col_courses is None or self._col_modules is None or self._col_items is None or self._col_jobs is None:
            return
        try:
            self._col_courses.create_index([("canvasCourseId", ASCENDING)], unique=True)
            self._col_modules.create_index([("courseId", ASCENDING)])
            self._col_items.create_index([("moduleId", ASCENDING)])
            self._col_jobs.create_index([("startedAt", ASCENDING)])
        except Exception as e:
            logger.log("ERROR", "Failed to create indexes", error=str(e))

    @retry(max_attempts=3, base_delay=1)
    def write_lms_course(self, course: LmsCourse, task_id: str, warnings: Optional[List[str]] = None) -> bool:
        """
        Shreds the course into a normalized schema: Courses -> Modules -> Items.
        Uses MongoDB transactions for atomicity.
        """
        self._ensure_connection()
        if self._client is None or self._col_courses is None:
             raise ConnectionError("MongoDB not connected")

        logger.log("INFO", "Writing normalized course to MongoDB", 
                   task_id=task_id, title=course.title)

        try:
            with self._client.start_session() as session:
                with session.start_transaction():
                    # 1. Create/Update Course root
                    course_id_in_db = self._upsert_course_root(course, session)
                    
                    # 2. Process Modules and Items
                    module_ids = []
                    for module in course.modules:
                        m_id = self._process_module(module, course_id_in_db, session)
                        module_ids.append(m_id)
                    
                    # 3. Update Course with module order
                    self._col_courses.update_one(
                        {"_id": course_id_in_db},
                        {"$set": {"modules": module_ids, "updatedAt": datetime.utcnow()}},
                        session=session
                    )

            self.update_job_status(task_id, "completed", progress=100, warnings=warnings)
            return True

        except Exception as e:
            logger.log("ERROR", "Normalized write failed", error=str(e), task_id=task_id)
            self.update_job_status(task_id, "failed", log_msg=f"DB Write Error: {str(e)}")
            raise e

    def _upsert_course_root(self, course: LmsCourse, session) -> ObjectId:
        """Upserts the root course document and returns its ObjectId."""
        if self._col_courses is None: raise ConnectionError()
        course_doc = {
            "title": course.title,
            "description": course.description,
            "status": "Published",
            "canvasCourseId": course.canvas_course_id,
            "courseCode": course.course_code or "DEFAULT",
            "slug": course.slug or self._slugify(course.title),
            "updatedAt": datetime.utcnow()
        }
        
        # Handle tenancy IDs
        for key, val in [("university", course.university), ("authorId", course.author_id)]:
            if val:
                try:
                    course_doc[key] = ObjectId(val)
                except:
                    course_doc[key] = val

        # Upsert
        res = self._col_courses.find_one_and_update(
            {"canvasCourseId": course.canvas_course_id},
            {"$set": course_doc, "$setOnInsert": {"createdAt": datetime.utcnow()}},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER,
            session=session
        )
        return res["_id"]

    def _process_module(self, module: LmsModule, course_id: ObjectId, session) -> ObjectId:
        """Saves a module and its items, returning the module's ObjectId."""
        if self._col_modules is None: raise ConnectionError()
        # 1. Prepare items
        item_ids = []
        
        # Lessons
        for lesson in module.lessons:
            item_doc = {
                "title": lesson.title,
                "type": "Lesson",
                "content": lesson.content,
                "courseId": course_id,
                "attachments": [{"name": url.split('/')[-1], "url": url} for url in lesson.asset_urls],
                "order": lesson.order
            }
            item_ids.append(self._save_item(item_doc, session))
            
        # Quizzes
        for quiz in module.quizzes:
            item_doc = {
                "title": quiz.title,
                "type": "Quiz",
                "courseId": course_id,
                "quizConfig": {
                    "timeLimit": quiz.time_limit_minutes or 0,
                    "attemptsAllowed": quiz.attempts_allowed,
                    "passingGrade": quiz.passing_grade_pct,
                    "shuffleQuestions": quiz.shuffle_questions
                },
                "questions": [self._question_to_dict(q) for q in quiz.questions],
                "order": quiz.order
            }
            item_ids.append(self._save_item(item_doc, session))

        # Assignments
        for assign in module.assignments:
            item_doc = {
                "title": assign.title,
                "type": "Assignment",
                "courseId": course_id,
                "instructions": assign.description,
                "assignmentConfig": {
                    "totalPoints": assign.points_possible,
                    "minPassPoints": assign.passing_points
                },
                "order": assign.order
            }
            item_ids.append(self._save_item(item_doc, session))

        # 2. Save Module
        module_doc = {
            "title": module.title,
            "description": module.description or "",
            "courseId": course_id,
            "items": item_ids,
            "order": module.order,
            "updatedAt": datetime.utcnow()
        }
        
        res = self._col_modules.insert_one(module_doc, session=session)
        return res.inserted_id

    def _save_item(self, item_doc: Dict[str, Any], session) -> ObjectId:
        """Saves a single course item (Lesson/Quiz/Assignment)."""
        if self._col_items is None: raise ConnectionError()
        # Ensure we don't exceed BSON limit per item (though unlikely for a single lesson)
        serialized = bson.BSON.encode(item_doc)
        if len(serialized) > 15 * 1024 * 1024:
             logger.log("WARNING", "Extremely large item detected", title=item_doc.get("title"))
             
        res = self._col_items.insert_one(item_doc, session=session)
        return res.inserted_id

    def _question_to_dict(self, q) -> Dict[str, Any]:
        return {
            "title": q.title,
            "type": q.question_type.value,
            "points": q.points,
            "options": [
                {"text": a.text, "isCorrect": a.is_correct} for a in q.answers
            ]
        }

    def _slugify(self, text: str) -> str:
        import re
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        return re.sub(r'[-\s]+', '-', text).strip('-')

    def create_job(self, task_id: str, s3_key: Optional[str] = None, checksum: Optional[str] = None):
        """Initialize a migration job record."""
        self._ensure_connection()
        if self._col_jobs is None: raise ConnectionError()
        job = {
            "_id": task_id,
            "status": "processing",
            "s3Key": s3_key,
            "package_checksum": checksum,
            "startedAt": datetime.utcnow(),
            "logs": [],
            "warnings": [],
            "progress": 0
        }
        self._col_jobs.update_one({"_id": task_id}, {"$set": job}, upsert=True)

    def update_job_status(self, 
                          task_id: str, 
                          status: str, 
                          log_msg: Optional[str] = None, 
                          progress: Optional[int] = None, 
                          warnings: Optional[List[str]] = None,
                          current_stage: Optional[str] = None):
        """Update existing job status, logs, progress, stage, and semantic warnings."""
        self._ensure_connection()
        if self._col_jobs is None: raise ConnectionError()
        
        update: Dict[str, Any] = {"$set": {"status": status, "updatedAt": datetime.utcnow()}}
        if progress is not None:
            update["$set"]["progress"] = progress
        if current_stage:
            update["$set"]["current_stage"] = current_stage
        push_updates: Dict[str, Any] = {}
        if log_msg:
            push_updates["logs"] = f"[{datetime.utcnow().isoformat()}] {log_msg}"
        if warnings:
            push_updates["warnings"] = {"$each": warnings}
        
        if push_updates:
            update["$push"] = push_updates
        
        if status in ("completed", "failed"):
            update["$set"]["completedAt"] = datetime.utcnow()

        self._col_jobs.update_one({"_id": task_id}, update)

    def find_by_checksum(self, checksum: str) -> Optional[Dict[str, Any]]:
        self._ensure_connection()
        if self._col_jobs is None: return None
        return self._col_jobs.find_one({"package_checksum": checksum})

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

def upload_to_mongodb(course: LmsCourse, task_id: str) -> bool:
    """
    Convenience wrapper for the pipeline orchestrator.
    """
    uploader = MongoDBUploader()
    return uploader.write_lms_course(course, task_id)
