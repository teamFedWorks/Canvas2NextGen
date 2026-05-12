"""
Test parsing of BS CS PHI-1114 from already-extracted folder.
"""
import sys
sys.path.insert(0, 'src')

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

from adapters.zip_adapter import ZipAdapter
from pathlib import Path

# Use the already-extracted directory
course_dir = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation')
print(f'Loading from: {course_dir}')
print('Exists?', course_dir.exists())

adapter = ZipAdapter()
canvas_course = adapter.load({'zip_path': str(course_dir)})

print(f'\nCourse: {canvas_course.title}')
print(f'Identifier: {canvas_course.identifier}')
print(f'Modules: {len(canvas_course.modules)}')
print(f'Pages: {len(canvas_course.pages)}')
print(f'Quizzes: {len(canvas_course.quizzes)}')
print(f'Discussions: {len(canvas_course.discussions)}')
print(f'Resources: {len(canvas_course.resources)}')

# Sample content from first page
if canvas_course.pages:
    p = canvas_course.pages[0]
    print(f'\nFirst page: {p.title}')
    body_preview = (p.body or '')[:200]
    print(f'Body preview: {body_preview}...')
