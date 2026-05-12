from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil, re

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_body_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

# Find the page "Syllabus 5306" and print any HTML snippet containing <a> tags or xid
for page in course.pages:
    if page.title == 'Syllabus 5306':
        body = page.body or ''
        # Replace some markers for readability
        # Print any anchor tags
        idx = body.lower().find('xid')
        if idx >= 0:
            print('Snippet around xid:', body[max(0,idx-100):idx+200])
        else:
            print('No xid found')
        print('\n--- Full body ---')
        print(body[:500])
        break

shutil.rmtree(extract_dir)
