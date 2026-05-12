"""
Source Classifier - Detects LMS type and version from export packages.

This is the FIRST step in the canonical pipeline.
Always run classification before any parsing begins.
"""

from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass
import zipfile
import xml.etree.ElementTree as ET

from models.canonical_models import SourcePlatform
from observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ClassificationResult:
    """Result of source classification"""
    platform: SourcePlatform
    confidence: float  # 0.0 to 1.0
    version: Optional[str] = None
    export_type: str = "course_package"
    detected_features: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.detected_features is None:
            self.detected_features = {}


class SourceClassifier:
    """
    Automatically detects the LMS source from export files.
    
    Detection strategy:
    1. Check for imsmanifest.xml (IMS-CC based: Canvas, Blackboard, Moodle)
    2. Analyze manifest namespace and structure
    3. Check for platform-specific files
    4. Return confidence score
    """
    
    # Confidence thresholds
    HIGH_CONFIDENCE = 0.9
    MEDIUM_CONFIDENCE = 0.7
    
    @staticmethod
    def classify_directory(course_dir: Path) -> ClassificationResult:
        """Classify an extracted course directory."""
        manifest_path = course_dir / "imsmanifest.xml"
        
        if not manifest_path.exists():
            logger.warning(f"No imsmanifest.xml found in {course_dir}")
            return ClassificationResult(
                platform=SourcePlatform.CUSTOM,
                confidence=0.3,
                export_type="unknown"
            )
        
        return SourceClassifier._classify_manifest(manifest_path)
    
    @staticmethod
    def classify_zip(zip_path: Path) -> ClassificationResult:
        """Classify a ZIP file by examining its manifest."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                manifest_files = [f for f in zf.namelist() if 'imsmanifest.xml' in f]
                if not manifest_files:
                    return ClassificationResult(
                        platform=SourcePlatform.CUSTOM,
                        confidence=0.2,
                        export_type="unknown"
                    )
                
                manifest_path = manifest_files[0]
                with zf.open(manifest_path) as mf:
                    # Read content to analyze
                    content = mf.read().decode('utf-8', errors='replace')
                    return SourceClassifier._classify_manifest_content(content)
        except Exception as e:
            logger.error(f"Error classifying ZIP {zip_path}: {e}")
            return ClassificationResult(
                platform=SourcePlatform.CUSTOM,
                confidence=0.1,
                export_type="unknown"
            )
    
    @staticmethod
    def _classify_manifest(manifest_path: Path) -> ClassificationResult:
        """Classify based on manifest file content."""
        try:
            with open(manifest_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            return SourceClassifier._classify_manifest_content(content)
        except Exception as e:
            logger.error(f"Error reading manifest {manifest_path}: {e}")
            return ClassificationResult(
                platform=SourcePlatform.CUSTOM,
                confidence=0.1,
                export_type="unknown"
            )
    
    @staticmethod
    def _classify_manifest_content(content: str) -> ClassificationResult:
        """Analyze manifest XML content to determine platform."""
        
        # Blackboard detection - look for bb: namespace
        if 'xmlns:bb="http://www.blackboard.com/content-packaging/' in content:
            # Check for Blackboard-specific elements
            bb_indicators = [
                'bb:file=',
                'bb:title=',
                '<CONTENTHANDLER',
                'resource/x-bb-folder',
                'course/x-bb-coursesetting',
            ]
            bb_score = sum(1 for ind in bb_indicators if ind in content)
            confidence = min(0.95, 0.7 + (bb_score * 0.05))
            
            return ClassificationResult(
                platform=SourcePlatform.BLACKBOARD,
                confidence=confidence,
                version=SourceClassifier._extract_bb_version(content),
                export_type="course_package",
                detected_features={"bb_score": bb_score}
            )
        
        # Canvas detection - standard IMS-CC with specific structure
        # Canvas uses imscanvas namespace rarely, usually plain IMS-CC
        canvas_indicators = [
            'xmlns:imsmd="http://www.imsglobal.org/xsd/imsmd_v1p2"',
            'web_resources/',
            'wiki_content/',
            'assessment_meta.xml',
        ]
        canvas_score = sum(1 for ind in canvas_indicators if ind in content)
        
        # Moodle detection - look for Moodle-specific structures
        moodle_indicators = [
            'mod/',
            'moodle_backup.xml',
            '"http://moodle.org/',
        ]
        moodle_score = sum(1 for ind in moodle_indicators if ind in content)
        
        # D2L Brightspace detection
        d2l_indicators = [
            'd2l_',
            'Brightspace',
            'desire2learn',
        ]
        d2l_score = sum(1 for ind in d2l_indicators if ind.lower() in content.lower())
        
        # Determine winner
        scores = {
            SourcePlatform.CANVAS: canvas_score,
            SourcePlatform.MOODLE: moodle_score,
            SourcePlatform.D2L_BRIGHTSPACE: d2l_score,
        }
        
        winner = max(scores, key=scores.get)
        max_score = scores[winner]
        
        if max_score == 0:
            # Generic IMS-CC - default to Canvas as most common
            return ClassificationResult(
                platform=SourcePlatform.CANVAS,
                confidence=0.6,
                export_type="imscc_package",
                detected_features={"generic_imscc": True}
            )
        
        confidence = min(0.95, 0.5 + (max_score * 0.1))
        
        return ClassificationResult(
            platform=winner,
            confidence=confidence,
            export_type="course_package",
            detected_features={k.value: v for k, v in scores.items()}
        )
    
    @staticmethod
    def _extract_bb_version(content: str) -> Optional[str]:
        """Extract Blackboard version if available."""
        # Look for version indicators in the manifest
        import re
        match = re.search(r'version["\s:=]+([0-9.]+)', content, re.I)
        return match.group(1) if match else None


def classify_source(source_path: Path) -> ClassificationResult:
    """
    Convenience function to classify a source file or directory.
    """
    if source_path.is_file() and source_path.suffix == '.zip':
        return SourceClassifier.classify_zip(source_path)
    return SourceClassifier.classify_directory(source_path)