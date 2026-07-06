#!/usr/bin/env python3
"""
Staging DB Diagnostic — diagnose why courses are not showing in the admin UI.
Run from CourseOnboarding directory:
  python scripts/diagnose_staging_courses.py

Delete this file after use.
"""
import os
import sys
from pathlib import Path
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

URI = os.getenv("PLATFORM_MONGODB_URI")
DB  = os.getenv("PLATFORM_MONGODB_DATABASE", "test")

if not URI:
    print("ERROR: PLATFORM_MONGODB_URI not set in .env", file=sys.stderr)
    sys.exit(1)

client  = MongoClient(URI)
db      = client[DB]
courses = db["courses"]
unis    = db["universities"]

print(f"\n{'='*52}")
print(f"   STAGING DB DIAGNOSTIC   db={DB}")
print(f"{'='*52}\n")

# ── 1. Basic counts ────────────────────────────────────────
total       = courses.count_documents({})
valid_uni   = courses.count_documents({"university": {"$type": "objectId"}})
null_uni    = courses.count_documents({"university": None})
missing_uni = courses.count_documents({"university": {"$exists": False}})
string_uni  = courses.count_documents({"university": {"$type": "string"}})
soft_del    = courses.count_documents({"deletedAt": {"$ne": None}})
active_ok   = courses.count_documents({"university": {"$type": "objectId"}, "deletedAt": None})

print("COURSES:")
print(f"  Total documents           : {total}")
print(f"  Valid ObjectId university : {valid_uni}")
print(f"  university = null         : {null_uni}")
print(f"  university field missing  : {missing_uni}")
print(f"  university = string (bad) : {string_uni}")
print(f"  soft-deleted (deletedAt)  : {soft_del}")
print(f"  Active + valid uni        : {active_ok}  ← what the admin table queries")

# ── 2. Status breakdown ────────────────────────────────────
print("\nSTATUS BREAKDOWN (all docs):")
for row in courses.aggregate([{"$group": {"_id": "$status", "count": {"$sum": 1}}}]):
    print(f'  "{row["_id"]}" : {row["count"]}')

# ── 3. University IDs actually stored on courses ───────────
print("\nUNIVERSITY IDs FOUND ON COURSES (top 20):")
for row in courses.aggregate([
    {"$match": {"university": {"$type": "objectId"}}},
    {"$group": {"_id": "$university", "count": {"$sum": 1}}},
    {"$sort":  {"count": -1}},
    {"$limit": 20},
]):
    print(f"  {row['_id']}  →  {row['count']} course(s)")

# ── 4. Universities collection ─────────────────────────────
uni_total = unis.count_documents({})
uni_list  = list(unis.find({}, {"_id": 1, "name": 1, "shortName": 1}))

print(f"\nUNIVERSITIES COLLECTION (total={uni_total}):")
if uni_list:
    for u in uni_list:
        print(f"  {u['_id']}  |  {u.get('name')}  |  {u.get('shortName')}")
else:
    print("  *** EMPTY — no universities in DB! ***")

# ── 5. Cross-check: orphaned university IDs ───────────────
uni_ids_on_courses = set(
    str(row["_id"])
    for row in courses.aggregate([
        {"$match": {"university": {"$type": "objectId"}}},
        {"$group": {"_id": "$university"}},
    ])
)
uni_ids_in_db = {str(u["_id"]) for u in uni_list}
orphaned = uni_ids_on_courses - uni_ids_in_db

if orphaned:
    print(f"\n⚠  ORPHANED UNI IDs (referenced by courses but NOT in universities coll):")
    for oid in orphaned:
        count = courses.count_documents({"university": ObjectId(oid)})
        print(f"  {oid}  →  {count} course(s) reference this missing university")
else:
    print("\n✓  All course university IDs exist in the universities collection.")

# ── 6. Sample active courses ───────────────────────────────
print("\nSAMPLE COURSES (first 5, valid uni, not deleted):")
sample = list(courses.find(
    {"university": {"$type": "objectId"}, "deletedAt": None},
    {"title": 1, "status": 1, "university": 1, "isPublic": 1, "deletedAt": 1}
).limit(5))
if sample:
    for c in sample:
        print(f"  _id={c['_id']} | status={c.get('status')!r} | "
              f"isPublic={c.get('isPublic')} | uni={c.get('university')}")
else:
    print("  *** No active courses with valid university found! ***")

# ── 7. Check known university IDs from .env ───────────────
known_ids = {
    "DEFAULT_UNIVERSITY_ID (SFC)": "69cb6cc4f1e9b0dda0713810",
    "WBU_UNIVERSITY_ID":           "69be64cd355271ea5c3da6b7",
}
print("\nKNOWN UNIVERSITY IDs FROM .env:")
for label, uid in known_ids.items():
    in_db     = unis.count_documents({"_id": ObjectId(uid)}) > 0
    n_courses = courses.count_documents({"university": ObjectId(uid)})
    print(f"  {label}")
    print(f"    id={uid}  exists_in_unis={in_db}  courses_linked={n_courses}")

print(f"\n{'='*52}\n")
client.close()
