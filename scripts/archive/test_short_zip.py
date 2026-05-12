import sys
sys.path.insert(0, 'src')
import ctypes
from ctypes import wintypes
import os, zipfile, tempfile, shutil
from pathlib import Path

# Windows API to get short path
def get_short_path(long_path):
    buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
    ctypes.windll.kernel32.GetShortPathNameW(str(long_path), buf, wintypes.MAX_PATH)
    return buf.value

long_zip = r'B:\EduvateHub\CourseOnboarding\storage\uploads\BS Computer Science\01 - PHI-1114 Logic and Argumentation.zip'
print('Long path:', long_path)
try:
    short_zip = get_short_path(long_zip)
    print('Short path:', short_zip)
except Exception as e:
    print('GetShortPathName failed:', e)
    sys.exit(1)

# Now check existence using short path (should have no spaces)
print('Exists?', os.path.exists(short_zip))

if os.path.exists(short_zip):
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(short_zip, 'r') as zf:
            zf.extractall(extract_dir)
        print('Extracted OK')
        from utils.format_detector import FormatDetector
        fmt = FormatDetector.detect(extract_dir)
        print('Format:', fmt.value)
        top = list(extract_dir.iterdir())
        print('Top items count:', len(top))
        for t in top[:10]:
            print(' ', t.name)
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
else:
    print('Short zip not found')
