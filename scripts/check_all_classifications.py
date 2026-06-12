import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from core.classifier import classify_source

def main():
    print("\n# Course Source Classification Audit")
    print("| Course Path | Detected Platform | Confidence | Version |")
    print("|:---|:---|:---|:---|")
    
    uploads = ROOT / "storage" / "uploads"
    paths = []
    
    # Traverse WBU, BS Information Technology, BS_Computer_Science
    for sub in uploads.iterdir():
        if sub.is_dir() and sub.name not in ["tutor_lms_output", ".git", "__pycache__", "lms_output", "csfiles", "ppg"]:
            for item in sub.iterdir():
                if item.is_dir():
                    # Add directories that contain imsmanifest.xml or are course dirs
                    if (item / "imsmanifest.xml").exists() or (item / ".bb-package-info").exists() or any(p.suffix == '.xml' for p in item.iterdir()):
                        paths.append(item)
                elif item.suffix == ".zip":
                    paths.append(item)
                    
    paths.sort(key=lambda p: str(p))
    
    for path in paths:
        rel_path = path.relative_to(uploads)
        try:
            res = classify_source(path)
            print(f"| {rel_path} | {res.platform.value} | {res.confidence:.2f} | {res.version or 'N/A'} |")
        except Exception as e:
            print(f"| {rel_path} | ERROR: {str(e)} | - | - |")

if __name__ == "__main__":
    main()
