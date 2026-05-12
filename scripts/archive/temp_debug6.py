from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil, re

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_checkbody_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

# Search which pages' bodies contain the嵌入 markers
for page in course.pages:
    body = page.body or ''
    if re.search(r'xid-\d+_\d+', body):
        print(f'Page "{page.title}" contains xid reference')
        # show snippet
        m = re.search(r'(?:@X@EmbeddedFile[^>]*xid-\d+_\d+[^<\s]*|xid-\d+_\d+)', body)
        if m:
            print(f'  Match: {m.group()[:150]}')
        else:
            print(f'  Body (first 200): {body[:200]}')

shutil.rmtree(extract_dir)
