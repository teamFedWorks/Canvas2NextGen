import sys
sys.path.insert(0, 'src')
from pathlib import Path
from core.stages.parser import Parser

course_dir = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation')
p = Parser(course_dir)
course, _ = p.parse()

search = ['Self-Introduction', 'Definitions', 'Reflection on the course', 'For an in class']
for mod in course.modules:
    for item in mod.items:
        if any(s.lower() in item.title.lower() for s in search):
            ref = getattr(item, '_content_ref', None)
            print(f"title={item.title[:55]}")
            print(f"  content_type={item.content_type}")
            print(f"  _content_ref={ref}")
            print(f"  identifier={item.identifier}")
            print()
