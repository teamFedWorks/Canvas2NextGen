import sys
sys.path.insert(0, 'src')

from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_check_empty_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

print('Pages with NO body content:')
for page in course.pages:
    body = (page.body or '').strip()
    if not body:
        print(f'  - {page.title}')

print(f'\nTotal pages: {len(course.pages)}')
print(f'Pages with body: {sum(1 for p in course.pages if (p.body or "").strip())}')
print(f'Pages without body: {sum(1 for p in course.pages if not (p.body or "").strip())}')

shutil.rmtree(extract_dir)
