import sys
sys.path.insert(0, 'src')
import os, zipfile, tempfile, shutil
from pathlib import Path

# Use extended-length path prefix to bypass normal path parsing
long_path = r'\\?\B:\EduvateHub\CourseOnboarding\storage\uploads\BS Computer Science\01 - PHI-1114 Logic and Argumentation.zip'
print('Testing extended path:', long_path)
print('Exists?', os.path.exists(long_path))
if os.path.exists(long_path):
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(long_path, 'r') as zf:
            zf.extractall(extract_dir)
        print('Extracted OK to', extract_dir)
        from utils.format_detector import FormatDetector
        fmt = FormatDetector.detect(extract_dir)
        print('Format:', fmt.value)
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
else:
    print('File not found via extended path')
