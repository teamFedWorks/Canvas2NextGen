#!/usr/bin/env python3
"""
Find and optionally clean course documents with invalid `university` references.

SAFE TO RUN: Dry-run by default. No data is changed unless --apply is passed.
Only touches documents with genuinely invalid university values (empty string,
non-ObjectId string, or missing field). Valid ObjectId references and null
values that were already intentionally set are left completely untouched.

Usage:
  # Preview — shows what would be changed, writes nothing
  python scripts/cleanup_invalid_course_universities.py

  # Apply — sets invalid university values to null
  python scripts/cleanup_invalid_course_universities.py --apply

  # Target a specific DB (overrides .env)
  python scripts/cleanup_invalid_course_universities.py --uri <MONGO_URI> --db <DB_NAME>

What it changes:
  university: ""          → null   (empty string — crashes Mongoose populate)
  university: "sometext"  → null   (non-ObjectId string)
  university: <missing>   → null   (field absent entirely)

What it does NOT touch:
  university: ObjectId("...")   — valid reference, untouched
  university: null              — already safe, untouched
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def is_valid_object_id(value: Any) -> bool:
    """Return True only for bson.ObjectId instances or valid 24-char hex strings."""
    if isinstance(value, ObjectId):
        return True
    if not isinstance(value, str):
        # None, int, dict, etc. — not a valid reference
        return False
    value = value.strip()
    return bool(value) and ObjectId.is_valid(value)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean invalid course.university references.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write cleanup changes. Default is dry-run (no writes).",
    )
    parser.add_argument(
        "--uri",
        default=os.getenv("MONGODB_URI") or os.getenv("ULCP_MONGODB_URI"),
        help="MongoDB connection URI (defaults to MONGODB_URI / ULCP_MONGODB_URI env var).",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("MONGODB_DATABASE") or os.getenv("ULCP_MONGODB_DATABASE") or "test",
        help="Database name (defaults to MONGODB_DATABASE / ULCP_MONGODB_DATABASE env var).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum bad examples to print in the report (default: 500).",
    )
    args = parser.parse_args()

    if not args.uri:
        print(
            "ERROR: Missing MongoDB URI. Set MONGODB_URI or ULCP_MONGODB_URI in .env, "
            "or pass --uri <connection_string>.",
            file=sys.stderr,
        )
        return 2

    client = MongoClient(args.uri)
    db = client[args.db]
    courses = db["courses"]

    # ── Step 1: Count total courses so we have a baseline ─────────────────────
    total_courses = courses.count_documents({})

    # ── Step 2: Find candidates — anything that isn't a proper ObjectId ───────
    # We cast a wide net here; is_valid_object_id() below does the precise check.
    bad_filter = {
        "$or": [
            {"university": {"$type": "string"}},   # any string (includes "")
            {"university": {"$exists": False}},      # field missing entirely
        ]
    }

    # ── Step 3: Verify each candidate and collect report data ─────────────────
    bad_courses_sample = []   # for reporting (capped by --limit)
    invalid_ids = []           # all IDs to fix (no cap)

    for course in courses.find(bad_filter, {"title": 1, "slug": 1, "university": 1}):
        val = course.get("university")
        if not is_valid_object_id(val):
            invalid_ids.append(course["_id"])
            if len(bad_courses_sample) < args.limit:
                bad_courses_sample.append(course)

    # ── Step 4: Also report null values (already safe, but worth knowing) ─────
    null_count = courses.count_documents({"university": None})

    # ── Step 5: Print report ───────────────────────────────────────────────────
    print("=" * 60)
    print(f"Database              : {args.db}")
    print(f"Total courses         : {total_courses}")
    print(f"  → invalid university: {len(invalid_ids)}  (will be set to null)")
    print(f"  → university is null: {null_count}         (already safe — untouched)")
    print(f"  → valid references  : {total_courses - len(invalid_ids) - null_count}")
    print("=" * 60)

    if bad_courses_sample:
        print(f"\nSample of invalid documents (showing up to {args.limit}):")
        for course in bad_courses_sample:
            print(
                f"  _id={course.get('_id')} | "
                f"slug={course.get('slug')!r} | "
                f"title={course.get('title')!r} | "
                f"university={course.get('university')!r}"
            )

    if not args.apply:
        print(
            "\n[DRY RUN] No changes written. "
            "Re-run with --apply to set invalid university values to null."
        )
        return 0

    if not invalid_ids:
        print("\nNothing to fix — no invalid course.university references found.")
        return 0

    # ── Step 6: Apply — set invalid references to null ────────────────────────
    # Uses $in on the pre-collected list of _ids so we only touch
    # exactly the documents we already validated above.
    print(f"\nApplying fix to {len(invalid_ids)} document(s)...")
    result = courses.update_many(
        {"_id": {"$in": invalid_ids}},
        {"$set": {"university": None}},
    )
    print(f"[OK] Modified {result.modified_count} course document(s).")

    # ── Step 7: Verify ────────────────────────────────────────────────────────
    remaining = 0
    for course in courses.find(bad_filter, {"university": 1}):
        if not is_valid_object_id(course.get("university")):
            remaining += 1

    if remaining == 0:
        print("[OK] Verification passed — no invalid university references remain.")
    else:
        print(
            f"[WARN] {remaining} invalid reference(s) still found after update. "
            "Check for concurrent writes or filter gaps."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
