"""
Extract all WARN item titles across all validation reports.
Groups them by category to identify patterns for nav placeholder fixes.
"""
import json
from pathlib import Path
from collections import Counter

outputs = Path("storage/outputs")
all_empty_titles = []
all_empty_assignments = []

for f in sorted(outputs.glob("validation_*.json")):
    data = json.loads(f.read_text())
    slug = data["slug"]
    for mod in data["module_results"]:
        for item in mod["items"]:
            if item["status"] == "WARN" and "No content body" in item.get("detail", ""):
                entry = (item["title"], item["item_type"], slug)
                if item["item_type"] == "Assignment":
                    all_empty_assignments.append(entry)
                else:
                    all_empty_titles.append(entry)

print("=" * 70)
print("EMPTY LESSONS (no content, no files)")
print("=" * 70)
title_counts = Counter(t for t, _, _ in all_empty_titles)
for title, count in sorted(title_counts.items(), key=lambda x: -x[1]):
    courses = set(s for t, _, s in all_empty_titles if t == title)
    print(f"  [{count}x] {title[:65]}")
    if count > 1:
        print(f"        in: {', '.join(sorted(courses))}")

print()
print("=" * 70)
print("EMPTY ASSIGNMENTS (no content, no files)")
print("=" * 70)
assign_counts = Counter(t for t, _, _ in all_empty_assignments)
for title, count in sorted(assign_counts.items(), key=lambda x: -x[1]):
    courses = set(s for t, _, s in all_empty_assignments if t == title)
    print(f"  [{count}x] {title[:65]}")
    if count > 1:
        print(f"        in: {', '.join(sorted(courses))}")
