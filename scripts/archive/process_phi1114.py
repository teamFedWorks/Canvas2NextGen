import sys
sys.path.insert(0, 'src')
import shutil, tempfile, zipfile, os
from pathlib import Path

# Source and destination with short names
src_zip = r'\\?\B:\EduvateHub\CourseOnboarding\storage\uploads\BS Computer Science\01 - PHI-1114 Logic and Argumentation.zip'
dst_dir = Path(tempfile.mkdtemp(prefix='phi1114_'))
print(f'Will extract to: {dst_dir}')

try:
    with zipfile.ZipFile(src_zip, 'r') as zf:
        zf.extractall(dst_dir)
    print('Extracted successfully')
    # Now try to ingest from dst_dir
    from adapters.zip_adapter import ZipAdapter
    adapter = ZipAdapter()
    canvas_course = adapter.load({'zip_path': str(dst_dir)})
    print(f'\nCourse: {canvas_course.title}')
    print(f'Modules: {len(canvas_course.modules)}')
    print(f'Pages: {len(canvas_course.pages)}')
    print(f'Quizzes: {len(canvas_course.quizzes)}')
    print(f'Resources: {len(canvas_course.resources)}')
except Exception as e:
    print(f'Error: {e}')
    import traceback; traceback.print_exc()
finally:
    if dst_dir.exists():
        shutil.rmtree(dst_dir, ignore_errors=True)
