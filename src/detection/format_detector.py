from pathlib import Path
from enum import Enum

class ExportFormat(Enum):
    IMSCC = "imscc"
    CANVAS_EXPORT = "canvas_export"
    UNKNOWN = "unknown"

class FormatDetector:
    """
    Detects the format of an unzipped Canvas course package.
    """
    
    @staticmethod
    def detect(extract_dir: Path) -> ExportFormat:
        """
        Detects format based on file presence.
        """
        # IMS Common Cartridge detection
        if (extract_dir / "imsmanifest.xml").exists():
            return ExportFormat.IMSCC
            
        # Canvas Course Export (.zip) detection
        if (extract_dir / "course_export.json").exists():
            return ExportFormat.CANVAS_EXPORT
            
        # Also check for module_meta.xml as a fallback/secondary indicator
        if (extract_dir / "modules" / "module_meta.xml").exists():
             return ExportFormat.CANVAS_EXPORT

        return ExportFormat.UNKNOWN
