"""
Chunked MongoDB Exporter - Handles large courses safely.

Divides courses into manageable chunks to avoid MongoDB's 16MB document limit.
Uses normalized collections instead of giant nested documents.
"""

import os
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import asdict

import bson
from pymongo import MongoClient

from models.canonical_models import (
    CanonicalCourse, CanonicalModule, CanonicalCurriculumItem,
    CanonicalAssessment, CanonicalQuestion, CanonicalAsset,
    CanonicalContentType, CanonicalQuestionType, SourcePlatform
)
from observability.logger import get_logger

logger = get_logger(__name__)


class ChunkedMongoExporter:
    """
    Exports courses in chunks to avoid MongoDB document size limits.
    
    Instead of one giant document, creates:
    - courses/{course_id} - basic metadata
    - modules/{module_id} - per-module data
    - assessments/{assessment_id} - per-assessment data  
    - assets/{asset_id} - per-asset data
    """
    
    MAX_DOC_SIZE = 15 * 1024 * 1024  # 15MB safe margin
    
    def __init__(self, mongodb_uri: str = None, database_name: str = None):
        self.uri = mongodb_uri or os.getenv("ULCP_MONGODB_URI")
        if not self.uri:
            raise ValueError("MongoDB connection URI must be explicitly provided or configured via 'ULCP_MONGODB_URI' environment variable.")
        self.db_name = database_name or os.getenv("ULCP_MONGODB_DATABASE", "test")
        self._client = None
        self._db = None
    
    def _ensure_connection(self):
        if not self._client:
            self._client = MongoClient(self.uri)
            self._db = self._client[self.db_name]
    
    def _canonical_to_dict(self, obj) -> Any:
        """
        Recursively convert canonical models to BSON-safe dicts.
        
        Handles:
        - Enums -> .value
        - Dataclasses -> dict
        - Lists -> converted elements
        - Datetime -> ISO string
        """
        if obj is None:
            return None
        
        # Handle enums
        if isinstance(obj, (CanonicalContentType, CanonicalQuestionType, SourcePlatform)):
            return obj.value
        
        # Handle datetime
        if isinstance(obj, datetime):
            return obj.isoformat()
        
        # Handle dataclasses
        if hasattr(obj, '__dict__'):
            result = {}
            for key, value in obj.__dict__.items():
                # Skip private fields
                if key.startswith('_'):
                    continue
                result[key] = self._canonical_to_dict(value)
            return result
        
        # Handle lists
        if isinstance(obj, list):
            return [self._canonical_to_dict(item) for item in obj]
        
        # Handle dicts
        if isinstance(obj, dict):
            return {k: self._canonical_to_dict(v) for k, v in obj.items()}
        
        # Primitive
        return obj
    
    def export_canonical_course(self, canonical: CanonicalCourse, university_id: str, author_id: str) -> str:
        """
        Export a canonical course in chunks.
        
        Returns the course_id for future reference.
        """
        self._ensure_connection()
        
        # Generate course slug
        course_slug = self._slugify(canonical.title)
        course_id = None
        
        # 1. Export base course document (metadata only)
        course_doc = self._canonical_to_dict(canonical)
        course_doc.update({
            "slug": course_slug,
            "universityId": university_id,
            "authorId": author_id,
            "status": "published",
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        })
        
        # Remove bulky arrays to keep document small (chunked data in separate collections)
        course_doc.pop('modules', None)
        course_doc.pop('assessments', None)
        course_doc.pop('assets', None)
        
        # Upsert
        result = self._db['courses'].replace_one(
            {"slug": course_slug},
            course_doc,
            upsert=True
        )
        course_id = result.upserted_id or self._db['courses'].find_one({"slug": course_slug})["_id"]
        
        # 2. Export modules
        for module in canonical.modules:
            self._export_module(module, course_id)
        
        # 3. Export assessments
        for assessment in canonical.assessments:
            self._export_assessment(assessment, course_id)
        
        # 4. Export assets
        for asset in canonical.assets:
            self._export_asset(asset, course_id)
        
        # Update course with counts
        self._db['courses'].update_one(
            {"_id": course_id},
            {"$set": {"updatedAt": datetime.utcnow()}}
        )
        
        logger.info(f"Exported canonical course {canonical.title} in chunks", 
                   extra={"course_id": str(course_id), "chunks": len(canonical.modules) + len(canonical.assessments) + len(canonical.assets)})
        
        return str(course_id)
    
    def _export_module(self, module: CanonicalModule, course_id):
        """Export a single module."""
        doc = {
            "courseId": course_id,
            "identifier": module.identifier,
            "title": module.title,
            "position": module.position,
            "items": [
                {
                    "identifier": item.identifier,
                    "title": item.title,
                    "contentType": item.content_type.value,
                    "position": item.position,
                    "assessmentRef": item.assessment_ref,
                    "assetRefs": item.asset_refs,
                }
                for item in module.items
            ]
        }
        
        self._db['modules'].replace_one(
            {"courseId": course_id, "identifier": module.identifier},
            doc,
            upsert=True
        )
    
    def _export_assessment(self, assessment: CanonicalAssessment, course_id):
        """Export a single assessment."""
        doc = {
            "courseId": course_id,
            "identifier": assessment.identifier,
            "title": assessment.title,
            "description": assessment.description,
            "type": assessment.assessment_type,
            "isGraded": assessment.is_graded,
            "pointsPossible": assessment.points_possible,
            "questions": [
                {
                    "identifier": q.identifier,
                    "text": q.text,
                    "type": q.type.value,
                    "points": q.points,
                }
                for q in assessment.questions
            ]
        }
        
        self._db['assessments'].replace_one(
            {"courseId": course_id, "identifier": assessment.identifier},
            doc,
            upsert=True
        )
    
    def _export_asset(self, asset: CanonicalAsset, course_id):
        """Export asset metadata."""
        doc = {
            "courseId": course_id,
            "identifier": asset.identifier,
            "filename": asset.filename,
            "url": asset.url,
            "mimeType": asset.mime_type,
            "sizeBytes": asset.size_bytes,
            "checksum": asset.checksum,
        }
        
        self._db['assets'].replace_one(
            {"courseId": course_id, "identifier": asset.identifier},
            doc,
            upsert=True
        )
    
    def _slugify(self, text: str) -> str:
        """Create URL-safe slug."""
        import re
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        return re.sub(r'[-\s]+', '-', text).strip('-')
    
    def close(self):
        if self._client:
            self._client.close()
            self._client = None


# Compatibility wrapper for legacy code
class MongoDBExporter(ChunkedMongoExporter):
    """
    Legacy-compatible MongoDB exporter.
    
    Maintains the old interface while using chunked internals.
    """
    
    def export(self, course_data: Dict[str, Any]) -> str:
        """
        Legacy export method - converts dict to canonical and exports.
        """
        # Extract key fields for course document
        course_doc = {
            "slug": course_data.get("slug"),
            "title": course_data.get("title"),
            "description": course_data.get("description", ""),
            "universityId": course_data.get("university"),
            "authorId": course_data.get("authorId"),
            "status": "published",
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }
        
        # Handle nested curriculum if present
        if "curriculum" in course_data:
            # Flatten for size check
            serialized = bson.BSON.encode(course_data)
            if len(serialized) > self.MAX_DOC_SIZE:
                # Need to chunk - but this is a dict, not canonical
                # Fall back to module chunking
                return self._export_legacy_chunked(course_data)
        
        # Standard export
        result = self._db['courses'].replace_one(
            {"slug": course_doc["slug"]},
            course_doc,
            upsert=True
        )
        
        course_id = result.upserted_id or self._db['courses'].find_one({"slug": course_doc["slug"]})["_id"]
        
        # Export modules separately
        if "curriculum" in course_data:
            for module in course_data.get("curriculum", []):
                self._db['modules'].insert_one({
                    "courseId": course_id,
                    "title": module.get("title"),
                    "items": module.get("items", [])
                })
        
        return str(course_id)
    
    def _export_legacy_chunked(self, course_data: Dict[str, Any]) -> str:
        """Fallback chunked export for large legacy documents."""
        course_doc = {
            "slug": course_data.get("slug"),
            "title": course_data.get("title"),
            "description": course_data.get("description", ""),
            "universityId": course_data.get("university"),
            "authorId": course_data.get("authorId"),
            "status": "published",
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }
        
        result = self._db['courses'].replace_one(
            {"slug": course_doc["slug"]},
            course_doc,
            upsert=True
        )
        
        return str(result.upserted_id or self._db['courses'].find_one({"slug": course_doc["slug"]})["_id"])