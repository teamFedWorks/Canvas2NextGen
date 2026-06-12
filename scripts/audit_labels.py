#!/usr/bin/env python3
"""
Deep classification audit for all SFC courses.
Shows what items have low confidence and why.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from pymongo import MongoClient
from collections import Counter

PIPELINE_URI = "mongodb+srv://venuthota95_db_user:UJsTwyo6qIP1vmoj@cluster0.zyucmd7.mongodb.net/?appName=Cluster0"
client = MongoClient(PIPELINE_URI)
db = client["lms_db"]

SFC_SLUGS = [
    "it-2410-01-25fa",       # Web Design          — 78 low-conf
    "it-2420-01-25sp",       # Multimedia          — 57 low-conf
    "it-2440-01-25fa",       # Scripting           — 79 low-conf
    "it-3101-01-25fa",       # IT Law              — 36 low-conf
    "it-3301-01-25fa",       # Project Management  — 66 low-conf
    "ent-1777-7a01-26sp",    # Design Thinking     — 31 low-conf
    "it-2510-01-25fa",       # Database            — 31 low-conf
    "it-2105-01-25fa",       # Programming II      — 33 low-conf
]

for slug in SFC_SLUGS:
    course = db.courses.find_one({"slug": slug}, {"title":1,"curriculum":1})
    if not course:
        continue

    print(f"\n{'='*70}")
    print(f"COURSE: {course['title']} ({slug})")
    print(f"{'='*70}")

    # Collect all low-conf items
    low_items = []
    type_counts = Counter()
    itype_counts = Counter()

    for mod in course.get("curriculum", []):
        for item in mod.get("items", []):
            t = item.get("type", "?")
            it = item.get("instructionalType", "")
            conf = item.get("classificationConfidence", 0)
            title = item.get("title", "")
            type_counts[t] += 1
            itype_counts[f"{t}/{it}"] += 1
            if conf < 0.80:
                low_items.append((conf, t, it, title))

    # Sort by confidence ascending
    low_items.sort()

    print(f"\nType distribution:")
    for t, n in type_counts.most_common():
        print(f"  {t:<14}: {n}")

    print(f"\nTop instructional types:")
    for combo, n in itype_counts.most_common(10):
        print(f"  {n:3d}x  {combo}")

    print(f"\nLow confidence items ({len(low_items)}) — sample of worst 20:")
    for conf, t, it, title in low_items[:20]:
        print(f"  ({conf:.2f}) [{t:12}] [{it:22}] {title[:55]}")

client.close()
print("\nDone.")
