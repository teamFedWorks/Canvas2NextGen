import sys
sys.path.insert(0, 'src')

from adapters.zip_adapter import ZipAdapter
from pathlib import Path

course_dir = Path('storage/uploads/BS_Computer_Science/01 - PHI-1114 Logic and Argumentation')
print(f'Loading from: {course_dir}')

adapter = ZipAdapter()
canvas_course = adapter.load({'zip_path': str(course_dir)})

print(f'\nCourse: {canvas_course.title}')
print(f'Identifier: {canvas_course.identifier}')
print(f'Modules: {len(canvas_course.modules)}')
print(f'Pages: {len(canvas_course.pages)}')
print(f'Quizzes: {len(canvas_course.quizzes)}')
print(f'Discussions: {len(canvas_course.discussions)}')
print(f'Resources: {len(canvas_course.resources)}')

print('\nModules:')
for i, mod in enumerate(canvas_course.modules, 1):
    items_with_body = [it for it in mod.items if (it.body or '').strip()]
    has_att = sum(1 for it in mod.items if getattr(it, 'attachments', []))
    print(f'  {i}. {mod.title} — {len(mod.items)} items, {len(items_with_body)} with body, {has_att} with attachments')
