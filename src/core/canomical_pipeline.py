"""
Canonical Migration Pipeline - The new ingestion architecture.

This pipeline follows the canonical model-first approach:
1. Classify source
2. Resolve manifest
3. Parse to canonical
4. Enrich content
5. Chunk and export
"""

from pathlib import Path
from typing import Optional, Callable
import tempfile
import shutil
import zipfile

from models.canonical_models import CanonicalCourse, SourcePlatform
from core.classifier import SourceClassifier, ClassificationResult
from core.manifest_resolver import ManifestResolver
from adapters.canonical_adapter import CanvasToCanonicalAdapter, BlackboardToCanonicalAdapter
from exporters.chunked_mongodb_exporter import ChunkedMongoExporter
from observability.logger import get_logger

logger = get_logger(__name__)


class CanonicalPipeline:
    """
    Modern ingestion pipeline using canonical normalization.
    
    Usage:
        pipeline = CanonicalPipeline(source_path="course.zip")
        result = pipeline.run(university_id="...", author_id="...")
    """
    
    def __init__(
        self,
        source_path: Path,
        on_progress: Optional[Callable] = None,
        university_id: Optional[str] = None,
        author_id: Optional[str] = None,
        task_id: Optional[str] = None
    ):
        self.source_path = Path(source_path)
        self.on_progress = on_progress
        self.university_id = university_id
        self.author_id = author_id
        self.task_id = task_id or f"task_{id(self)}"
        self._extract_dir: Optional[Path] = None
        
    def run(self) -> dict:
        """
        Execute the canonical pipeline.
        
        Returns:
            Dict with status, course_id, and any warnings.
        """
        self._notify("starting", 5, "Initializing canonical pipeline...")
        
        try:
            # Step 1: Classify source
            classification = self._classify_source()
            if classification.platform == SourcePlatform.CUSTOM and classification.confidence < 0.5:
                return {"status": "failed", "error": f"Unknown source type (confidence: {classification.confidence})"}
            
            self._notify("classifying", 10, f"Detected {classification.platform.value} (confidence: {classification.confidence:.2f})")
            
            # Step 2: Extract if needed
            course_dir = self._prepare_source()
            
            # Step 3: Resolve manifest dependencies
            self._notify("resolving", 15, "Building dependency graph...")
            resolver = ManifestResolver(course_dir)
            resolver.resolve()
            
            # Step 4: Parse to canonical
            self._notify("parsing", 30, "Converting to canonical model...")
            canonical = self._parse_canonical(course_dir, classification)
            
            # Step 5: Enrich content
            self._notify("enriching", 60, "Enriching content...")
            canonical = self._enrich(canonical)
            
            # Step 6: Export in chunks
            self._notify("exporting", 80, "Exporting to MongoDB...")
            exporter = ChunkedMongoExporter()
            try:
                course_id = exporter.export_canonical_course(
                    canonical,
                    self.university_id,
                    self.author_id
                )
            finally:
                exporter.close()
            
            self._notify("complete", 100, "Pipeline complete")
            
            return {
                "status": "success",
                "course_id": course_id,
                "title": canonical.title,
                "warnings": canonical.parsing_warnings,
                "platform": classification.platform.value
            }
            
        except Exception as e:
            logger.exception("Canonical pipeline failed")
            return {"status": "failed", "error": str(e)}
            
        finally:
            self._cleanup()
    
    def _classify_source(self) -> ClassificationResult:
        """Detect the LMS source type."""
        if self.source_path.suffix == '.zip':
            return SourceClassifier.classify_zip(self.source_path)
        return SourceClassifier.classify_directory(self.source_path)
    
    def _prepare_source(self) -> Path:
        """Extract ZIP if needed, return course directory."""
        if self.source_path.is_file() and self.source_path.suffix == '.zip':
            self._extract_dir = Path(tempfile.mkdtemp(prefix="canonical_"))
            
            with zipfile.ZipFile(self.source_path, 'r') as zf:
                zf.extractall(self._extract_dir)
            
            # Handle single root folder
            items = list(self._extract_dir.iterdir())
            if len(items) == 1 and items[0].is_dir():
                self._extract_dir = items[0]
            
            return self._extract_dir
        
        return self.source_path
    
    def _parse_canonical(self, course_dir: Path, classification: ClassificationResult) -> CanonicalCourse:
        """Parse using appropriate adapter."""
        payload = {"zip_path": str(course_dir)}
        
        if classification.platform == SourcePlatform.CANVAS:
            adapter = CanvasToCanonicalAdapter(course_dir)
        elif classification.platform == SourcePlatform.BLACKBOARD:
            adapter = BlackboardToCanonicalAdapter(course_dir)
        else:
            adapter = CanvasToCanonicalAdapter(course_dir)  # Default fallback
        
        return adapter.load(payload)
    
    def _enrich(self, canonical: CanonicalCourse) -> CanonicalCourse:
        """Apply content enrichment."""
        # Placeholder for enrichment logic
        # - HTML sanitization
        # - Metadata inference
        # - Semantic tagging
        return canonical
    
    def _notify(self, stage: str, progress: int, message: str):
        """Progress callback."""
        logger.info(f"Pipeline: {stage} - {message}", extra={"progress": progress})
        if self.on_progress:
            self.on_progress(stage, progress, message)
    
    def _cleanup(self):
        """Clean up temporary directories."""
        if self._extract_dir and self._extract_dir.exists():
            shutil.rmtree(self._extract_dir, ignore_errors=True)