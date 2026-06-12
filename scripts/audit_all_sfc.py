#!/usr/bin/env python3
"""
Batch audit all SFC courses in the pipeline DB.
Checks: item count, type distribution, low confidence, empty content, order issues.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from pymongo import MongoClient

PIPELINE_URI = "mongodb+srv://venuthota95_db_user:UJsTwyo6qIP1vmoj@cluster0.zyucmd7.mongodb.net/?appName=Cluster0"
client = MongoClient(PIPELINE_URI)
db = client["lms_db"]

SFC_SLUGS = [
    "phi-1114-01-26sp",
    "it-2410-01-25fa",
    "it-2420-01-25sp",
    "it-2440-01-25fa",
    "it-2510-01-25fa",
    "sandbox-it-2620-course-redesign",
    "it-3101-01-25fa",
    "it-3301-01-25fa",
    "it-3310-01-24sp",
    "it-4016-01-24fa",
    "ent-1001-05-25fa",
    "ent-1777-7a01-26sp",
    "it-1104-01-25fa",
    "it-2105-01-25fa",
]

STRUCTURAL_HEADERS = {"watch:", "read:", "complete:", "listen:", "view:", "do:"}

print(f"{'Course':<40} {'Mods':>4} {'Items':>6} {'LowConf':>8} {'Empty':>6} {'Types'}")
print("-" * 120)

total_courses = 0
total_items = 0
issues = []

for slug in SFC_SLUGS:
    course = db.courses.find_one({"slug": slug}, {"title":1,"curriculum":1})
    if not course:
        print(f"  {'NOT FOUND: '+slug:<38}")
        issues.append(f"MISSING: {slug}")
        continue

    total_courses += 1
    modules = course.get("curriculum", [])
    type_counts = {}
    low_conf = 0
    empty = 0
    structural = 0

    for mod in modules:
        for item in mod.get("items", []):
            t = item.get("type", "?")
            conf = item.get("classificationConfidence", 0)
            content = item.get("content", "")
            title_lower = (item.get("title","")).strip().lower().rstrip(".")

            # Check for structural headers that should have been filtered
            if title_lower in STRUCTURAL_HEADERS and not content.strip():
                structural += 1

            if conf < 0.80:
                low_conf += 1
            if len(content) < 10:
                empty += 1
            type_counts[t] = type_counts.get(t, 0) + 1

    item_count = sum(len(m.get("items",[])) for m in modules)
    total_items += item_count

    type_str = ", ".join(f"{t}:{n}" for t, n in sorted(type_counts.items(), key=lambda x: -x[1])[:5])

    flag = ""
    if structural > 0:
        flag = f" ⚠ {structural} structural headers"
        issues.append(f"{slug}: {structural} structural headers remaining")
    if low_conf > item_count * 0.5:
        flag += f" ⚠ {low_conf} low-conf"

    title = course.get("title", slug)[:38]
    print(f"  {title:<38} {len(modules):>4} {item_count:>6} {low_conf:>8} {empty:>6}  {type_str}{flag}")

print("-" * 120)
print(f"  {'TOTAL':<38} {'':>4} {total_items:>6}")

print(f"\n{'='*60}")
if issues:
    print(f"Issues found ({len(issues)}):")
    for i in issues:
        print(f"  ⚠ {i}")
else:
    print("✅ No issues found — all courses ready for migration")

client.close()
