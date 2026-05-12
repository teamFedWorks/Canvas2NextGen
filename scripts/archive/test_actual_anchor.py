import sys
sys.path.insert(0, 'src')
from bs4 import BeautifulSoup
import json, re

# The raw body from adapter's _BBContentReader (after _clean_bb_html)
# This is what was printed earlier: '<div ... data-bbfile="{"linkName":"MGMT 5306 VC01 SP1 26.pdf","displayName":"MGMT 5306 VC01 SP1 26.pdf","mimeType":"application/pdf","alternativeText":"MGMT 5306 VC01 SP1 26.pdf","render":"inline"}" href="bbcswebdav/xid-41796952_1"></a></div>'
# That body had unescaped quotes inside attribute, which is invalid. But the actual body might have been different?

# Let's extract the actual body from the parsed course using our earlier script
from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_rawbody_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

# Get Syllabus page body as stored by adapter
for page in course.pages:
    if page.title == 'Syllabus 5306':
        raw_body = page.body
        print('Raw body snippet:', raw_body[:200])
        # Parse with BS
        soup = BeautifulSoup(raw_body, 'html.parser')
        a = soup.find('a')
        if a:
            print('a tag:', a)
            print('data-bbfile attr:', repr(a.get('data-bbfile')))
        break

shutil.rmtree(extract_dir)
