from pathlib import Path
from enum import Enum

class ExportFormat(Enum):
    IMSCC        = "imscc"
    BLACKBOARD   = "blackboard"
    CANVAS_EXPORT = "canvas_export"
    UNKNOWN      = "unknown"

class FormatDetector:
    """
    Detects the format of an unzipped course package.
    Supports Canvas IMS-CC, Blackboard Learn Ultra, and Canvas native exports.
    """

    @staticmethod
    def detect(extract_dir: Path) -> ExportFormat:
        """
        Detects format based on file presence and manifest content.
        """
        manifest = extract_dir / "imsmanifest.xml"

        if manifest.exists():
            # Distinguish Canvas IMS-CC from Blackboard by manifest content
            try:
                content = manifest.read_text(encoding="utf-8", errors="replace")
                if "blackboard.com/content-packaging" in content:
                    return ExportFormat.BLACKBOARD
            except Exception:
                pass
            return ExportFormat.IMSCC

        # Canvas Course Export (.zip) detection
        if (extract_dir / "course_export.json").exists():
            return ExportFormat.CANVAS_EXPORT

        # Also check for module_meta.xml as a fallback/secondary indicator
        if (extract_dir / "modules" / "module_meta.xml").exists():
            return ExportFormat.CANVAS_EXPORT

        return ExportFormat.UNKNOWN
