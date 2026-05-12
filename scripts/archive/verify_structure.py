import sys
sys.path.insert(0, 'src')

from adapters.blackboard_adapter import BlackboardAdapter
from pathlib import Path
import zipfile, tempfile, shutil

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_count_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

adapter = BlackboardAdapter()
course = adapter._parse(extract_dir, {'zip_path': zip_path})

print('Course structure:')
print(f'Title: {course.title}')
print(f'Identifier: {course.identifier}')
print(f'Modules: {len(course.modules)}')
print(f'Pages: {len(course.pages)}')
print(f'Quizzes: {len(course.quizzes)}')
print(f'Discussions: {len(course.discussions)}')

print('\nModules:')
for i, m in enumerate(course.modules, 1):
    print(f'  {i}. {m.title} — {len(m.items)} items')

print(f'\nTotal curriculum items: {sum(len(m.items) for m in course.modules)}')

shutil.rmtree(extract_dir)
