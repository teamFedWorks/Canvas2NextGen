#!/usr/bin/env python3
"""
Staging DB Fix — addresses the three issues found by diagnose_staging_courses.py

Run dry-run first (default):
  python scripts/fix_staging_db.py

Then apply:
  python scripts/fix_staging_db.py --apply

What this does:
  1. Sets university: "" → null on the 1 course with the bad string value
     (this is the crash cause — CastError on University.find $in)
  2. Inserts the missing WBU university document so the 1 course referencing
     69be64cd355271ea5c3da6b7 resolves correctly
  3. Sets shortName on St. Francis College if it's missing
"""
import argparse
import os
import sys
from datetime import datetime, timezone
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

# ── Known IDs ─────────────────────────────────────────────
SFC_ID = ObjectId("69cb6cc4f1e9b0dda0713810")   # St. Francis College — exists
WBU_ID = ObjectId("69be64cd355271ea5c3da6b7")   # WBU — MISSING from unis coll

def run(apply: bool):
    now = datetime.now(timezone.utc)
    tag = "[DRY RUN]" if not apply else "[APPLY]"

    print(f"\n{'='*54}")
    print(f"  STAGING DB FIX   db={DB}   mode={'APPLY' if apply else 'DRY RUN'}")
    print(f"{'='*54}\n")

    # ──────────────────────────────────────────────────────────
    # FIX 1: course with university: "" → null
    # ──────────────────────────────────────────────────────────
    bad = list(courses.find(
        {"university": {"$type": "string"}},
        {"_id": 1, "title": 1, "slug": 1, "university": 1}
    ))
    print(f"FIX 1 — Courses with university stored as a string (crash cause):")
    if bad:
        for c in bad:
            print(f"  _id={c['_id']} | slug={c.get('slug')!r} | university={c.get('university')!r}")
        if apply:
            ids = [c["_id"] for c in bad]
            result = courses.update_many(
                {"_id": {"$in": ids}},
                {"$set": {"university": None}}
            )
            print(f"  {tag} Set university=null on {result.modified_count} document(s). ✓")
        else:
            print(f"  {tag} Would set university=null on {len(bad)} document(s).")
    else:
        print(f"  Nothing to fix — no string university values found.")

    # ──────────────────────────────────────────────────────────
    # FIX 2: Insert missing WBU university document
    # ──────────────────────────────────────────────────────────
    print(f"\nFIX 2 — Missing WBU university document (id={WBU_ID}):")
    wbu_courses = courses.count_documents({"university": WBU_ID})
    wbu_exists  = unis.count_documents({"_id": WBU_ID}) > 0

    print(f"  Courses referencing WBU: {wbu_courses}")
    print(f"  WBU exists in unis coll: {wbu_exists}")

    if wbu_exists:
        print(f"  Nothing to fix — WBU university document already exists.")
    else:
        wbu_doc = {
            "_id":       WBU_ID,
            "name":      "Wayland Baptist University",
            "shortName": "WBU",
            "slug":      "wayland-baptist-university",
            "status":    "Active",
            "createdAt": now,
            "updatedAt": now,
        }
        if apply:
            unis.insert_one(wbu_doc)
            print(f"  {tag} Inserted WBU university document. ✓")
        else:
            print(f"  {tag} Would insert: {wbu_doc}")

    # ──────────────────────────────────────────────────────────
    # FIX 3: St. Francis College shortName
    # ──────────────────────────────────────────────────────────
    print(f"\nFIX 3 — St. Francis College shortName (id={SFC_ID}):")
    sfc = unis.find_one({"_id": SFC_ID}, {"name": 1, "shortName": 1})
    if not sfc:
        print(f"  SFC not found in universities collection — skipping.")
    elif sfc.get("shortName"):
        print(f"  Already has shortName={sfc['shortName']!r} — nothing to fix.")
    else:
        print(f"  shortName is missing/empty for '{sfc.get('name')}'")
        if apply:
            unis.update_one({"_id": SFC_ID}, {"$set": {"shortName": "SFC", "updatedAt": now}})
            print(f"  {tag} Set shortName='SFC' on St. Francis College. ✓")
        else:
            print(f"  {tag} Would set shortName='SFC'.")

    # ──────────────────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────────────────
    print(f"\n{'='*54}")
    if not apply:
        print("DRY RUN complete — no changes written.")
        print("Re-run with --apply to apply all fixes.")
    else:
        print("All fixes applied.")
        # Quick verify
        remaining_bad = courses.count_documents({"university": {"$type": "string"}})
        wbu_now       = unis.count_documents({"_id": WBU_ID})
        sfc_now       = unis.find_one({"_id": SFC_ID}, {"shortName": 1})
        print(f"\nVERIFICATION:")
        print(f"  String university courses remaining : {remaining_bad}  (should be 0)")
        print(f"  WBU university in unis coll         : {wbu_now}  (should be 1)")
        print(f"  SFC shortName                       : {sfc_now.get('shortName')!r}  (should be 'SFC')")
    print(f"{'='*54}\n")

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes. Default is dry-run.")
    args = parser.parse_args()
    run(apply=args.apply)
