import os
from pathlib import Path

root = Path('storage/uploads')
courses = sorted([d.name for d in root.iterdir() if d.is_dir()])

image_exts = {'.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.bmp', '.ico'}
doc_exts = {'.pdf', '.docx', '.doc', '.xlsx', '.xls', '.txt', '.html', '.qti', '.xml.qti'}
video_exts = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
ppt_exts = {'.pptx', '.ppt'}

print("Course,XML,PPT,Images,Documents,Videos,Other")

for course in courses:
    course_dir = root / course
    stats = {'xml': 0, 'ppt': 0, 'images': 0, 'documents': 0, 'videos': 0, 'other': 0}
    
    for f in course_dir.rglob('*'):
        if f.is_file():
            ext = f.suffix.lower()
            if ext == '.xml':
                stats['xml'] += 1
            elif ext in ppt_exts:
                stats['ppt'] += 1
            elif ext in image_exts:
                stats['images'] += 1
            elif ext in doc_exts:
                stats['documents'] += 1
            elif ext in video_exts:
                stats['videos'] += 1
            else:
                stats['other'] += 1
    
    print(f"{course},{stats['xml']},{stats['ppt']},{stats['images']},{stats['documents']},{stats['videos']},{stats['other']}")