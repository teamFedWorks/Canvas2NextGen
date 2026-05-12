import sys
sys.path.insert(0, 'src')

from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil, re

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_attach_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

# Count pages with attachment links (data-bbfile or href with xid)
attach_count = 0
for page in course.pages:
    body = page.body or ''
    if re.search(r'data-bbfile|bbcswebdav/xid-', body):
        attach_count += 1

print(f'Pages with attachment links: {attach_count}')
print(f'Total pages: {len(course.pages)}')

shutil.rmtree(extract_dir)
