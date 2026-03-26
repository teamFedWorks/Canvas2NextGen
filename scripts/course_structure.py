import os
from pathlib import Path
import json

root = Path('storage/uploads')
courses = sorted([d.name for d in root.iterdir() if d.is_dir()])

report = {
    "generated_at": "2026-03-25T06:00:00Z",
    "courses": []
}

for course in courses:
    course_dir = root / course
    course_data = {
        "name": course,
        "manifest": (course_dir / 'imsmanifest.xml').exists(),
        "course_settings": (course_dir / 'course_settings').is_dir(),
        "quiz": (course_dir / 'quiz').is_dir(),
        "web_resources": (course_dir / 'web_resources').is_dir(),
        "wiki_content": (course_dir / 'wiki_content').is_dir(),
        "non_cc_assessments": (course_dir / 'non_cc_assessments').is_dir(),
    }
    
    # Count all items
    course_data["total_items"] = sum(1 for _ in course_dir.rglob("*") if _.is_file())
    
    # Check modules
    modules = [d for d in course_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    course_data["modules_count"] = len(modules)
    
    report["courses"].append(course_data)

print(json.dumps(report, indent=2))