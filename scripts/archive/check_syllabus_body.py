import sys
sys.path.insert(0, 'src')

from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_body_check_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

# Look for Syllabus 5306 page body
for page in course.pages:
    if page.title == 'Syllabus 5306':
        print('Body raw (first 500):')
        print(repr(page.body[:500]))
        break

shutil.rmtree(extract_dir)
