"""
Idempotency Service - Content-based deduplication and replay protection.

Ensures that:
1. Duplicate uploads are detected before processing
2. Retries don't create duplicates
3. Partial failures can be safely retried
4. Content changes are detected (for cache invalidation)
"""

import hashlib
import json
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from dataclasses import dataclass

from observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ContentHash:
    """Content hash with algorithm."""
    algorithm: str = "sha256"
    value: str = ""
    
    @classmethod
    def from_bytes(cls, data: bytes, algorithm: str = "sha256") -> 'ContentHash':
        """Compute hash from bytes."""
        h = hashlib.new(algorithm)
        h.update(data)
        return cls(algorithm=algorithm, value=h.hexdigest())
    
    @classmethod
    def from_file(cls, path: Path, algorithm: str = "sha256") -> 'ContentHash':
        """Compute hash from file."""
        h = hashlib.new(algorithm)
        with open(path, 'rb') as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return cls(algorithm=algorithm, value=h.hexdigest())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], algorithm: str = "sha256") -> 'ContentHash':
        """Compute hash from dictionary (canonical course, etc.)."""
        # Sort keys for consistent hashing
        serialized = json.dumps(data, sort_keys=True, separators=(',', ':'), default=str)
        return cls.from_bytes(serialized.encode('utf-8'), algorithm)
    
    def __str__(self):
        return f"{self.algorithm}:{self.value}"
    
    def __eq__(self, other):
        if not isinstance(other, ContentHash):
            return False
        return self.algorithm == other.algorithm and self.value == other.value


class IdempotencyKey:
    """
    Composite key for deduplication across multiple dimensions.
    """
    
    def __init__(
        self,
        source_platform: str,
        source_course_id: str,
        manifest_hash: ContentHash,
        content_hash: Optional[ContentHash] = None,
        version: str = "1.0"
    ):
        self.source_platform = source_platform
        self.source_course_id = source_course_id
        self.manifest_hash = manifest_hash
        self.content_hash = content_hash
        self.version = version
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "platform": self.source_platform,
            "source_id": self.source_course_id,
            "manifest_hash": str(self.manifest_hash),
            "content_hash": str(self.content_hash) if self.content_hash else None,
            "version": self.version
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IdempotencyKey':
        manifest_hash = ContentHash(
            algorithm=data["manifest_hash"].split(":")[0],
            value=data["manifest_hash"].split(":")[1]
        )
        content_hash = None
        if data.get("content_hash"):
            ch_parts = data["content_hash"].split(":")
            content_hash = ContentHash(algorithm=ch_parts[0], value=ch_parts[1])
        
        return cls(
            source_platform=data["platform"],
            source_course_id=data["source_id"],
            manifest_hash=manifest_hash,
            content_hash=content_hash,
            version=data.get("version", "1.0")
        )


class IdempotencyService:
    """
    Service for tracking and detecting duplicate content.
    
    Uses multiple strategies:
    1. Manifest hash - detect exact re-uploads
    2. Content hash - detect semantic duplicates  
    3. Source ID + timestamp - detect version changes
    
    Storage backend: MongoDB collection 'idempotency_keys'
    """
    
    def __init__(self, mongodb_uri: Optional[str] = None):
        from exporters.mongodb_exporter import MongoDBExporter
        self.db = MongoDBExporter(mongodb_uri)
    
    def compute_course_hashes(
        self, 
        course_dir: Path, 
        platform: str, 
        source_id: str
    ) -> Tuple[ContentHash, ContentHash]:
        """
        Compute manifest and content hashes for a course.
        
        Returns:
            (manifest_hash, content_hash)
        """
        # 1. Manifest hash - deterministic based on manifest structure
        manifest_path = course_dir / "imsmanifest.xml"
        if manifest_path.exists():
            manifest_hash = ContentHash.from_file(manifest_path)
        else:
            # Fallback: hash all XML files
            import zipfile
            manifest_hash = ContentHash.from_bytes(b"no_manifest")
        
        # 2. Content hash - hash all content files
        # Strategy: hash all webcontent files in order
        content_hasher = hashlib.sha256()
        
        # Collect all content files
        content_files = []
        for pattern in ["wiki_content/**", "web_resources/**", "*.xml", "*.html"]:
            content_files.extend(course_dir.glob(pattern))
        
        # Sort for deterministic ordering
        content_files.sort(key=lambda p: str(p.relative_to(course_dir)))
        
        for file_path in content_files:
            if file_path.is_file():
                with open(file_path, 'rb') as f:
                    content_hasher.update(f.read())
        
        content_hash = ContentHash(algorithm="sha256", value=content_hasher.hexdigest())
        
        return manifest_hash, content_hash
    
    def is_duplicate(
        self, 
        platform: str, 
        source_course_id: str, 
        manifest_hash: ContentHash,
        content_hash: Optional[ContentHash] = None
    ) -> Optional[str]:
        """
        Check if this course has been ingested before.
        
        Returns:
            Existing course_id if duplicate, None otherwise.
        """
        self.db._ensure_connection()
        
        # Build query
        query = {
            "platform": platform,
            "source_course_id": source_course_id,
            "manifest_hash": str(manifest_hash),
        }
        
        if content_hash:
            query["content_hash"] = str(content_hash)
        
        result = self.db._db['idempotency_keys'].find_one(query)
        
        if result:
            logger.info("Duplicate course detected", 
                       extra={"platform": platform, "source_id": source_course_id,
                              "existing_course_id": result.get("course_id")})
            return result.get("course_id")
        
        return None
    
    def register_ingestion(
        self,
        key: IdempotencyKey,
        course_id: str,
        job_id: str,
        metadata: Dict[str, Any] = None
    ):
        """Record a successful ingestion for future duplicate detection."""
        self.db._ensure_connection()
        
        doc = key.to_dict()
        doc.update({
            "course_id": course_id,
            "job_id": job_id,
            "ingested_at": datetime.utcnow(),
            "metadata": metadata or {},
        })
        
        self.db._db['idempotency_keys'].insert_one(doc)
        logger.info("Registered idempotency key", 
                   extra={"key": str(key), "course_id": course_id})
    
    def get_previous_ingestion(self, key: IdempotencyKey) -> Optional[Dict[str, Any]]:
        """Get previous ingestion record for this key."""
        self.db._ensure_connection()
        return self.db._db['idempotency_keys'].find_one(key.to_dict())
    
    def invalidate_previous(self, key: IdempotencyKey):
        """Invalidate previous ingestion (for forced re-ingest)."""
        self.db._ensure_connection()
        self.db._db['idempotency_keys'].delete_many(key.to_dict())
    
    def compute_semantic_hash(self, canonical_course) -> ContentHash:
        """
        Compute a semantic hash that's resilient to insignificant changes.
        
        Strips out:
        - Timestamps
        - UUIDs
        - Non-semantic whitespace
        
        This allows detecting true duplicates vs. re-uploads with minor changes.
        """
        from models.canonical_models import CanonicalCourse
        
        # Extract semantic fields only
        semantic_dict = {
            "title": canonical_course.title,
            "modules": [
                {
                    "title": m.title,
                    "items": [
                        {
                            "title": i.title,
                            "type": i.content_type.value,
                            "body_hash": hashlib.sha256((i.body or "").encode()).hexdigest()[:16]
                        }
                        for i in m.items
                    ]
                }
                for m in canonical_course.modules
            ],
            "assessments": [
                {
                    "title": a.title,
                    "questions": [
                        {"text": q.text, "type": q.type.value}
                        for q in a.questions
                    ]
                }
                for a in canonical_course.assessments
            ]
        }
        
        return ContentHash.from_dict(semantic_dict)