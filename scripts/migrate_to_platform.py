#!/usr/bin/env python3
"""
migrate_to_platform.py
======================
Migrates the freshly-ingested WBU course from the CourseOnboarding pipeline DB
(cluster0.zyucmd7 / lms_db) into the NextGen-Backend platform DB
(cluster-staging.jsxinqf / test).

What it does
------------
1. Reads the source course from the pipeline DB
2. Transforms the document to match the platform's Mongoose schema
3. Upserts the course into the platform DB (keyed on slug to be idempotent)
4. Migrates related assessments (quizzes) from pipeline assessments collection
5. Prints a summary of what was written

Run:
    python scripts/migrate_to_platform.py
    python scripts/migrate_to_platform.py --dry-run   # preview only, no writes
    python scripts/migrate_to_platform.py --course-id 69f83af9d7082df5d618ff5d
"""

import sys
import os
import argparse
import json
import copy
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from pymongo import MongoClient, UpdateOne
import bson

# ── Connection strings ────────────────────────────────────────────────────────
PIPELINE_URI = os.getenv("ULCP_MONGODB_URI")
PLATFORM_URI = os.getenv("PLATFORM_MONGODB_URI")
PLATFORM_DB  = os.getenv("PLATFORM_MONGODB_DATABASE", "test")
PIPELINE_DB  = os.getenv("ULCP_MONGODB_DATABASE", "test")

if not PIPELINE_URI:
    raise RuntimeError("ULCP_MONGODB_URI is not configured in the environment.")
if not PLATFORM_URI:
    raise RuntimeError("PLATFORM_MONGODB_URI is not configured in the environment.")

# ── Helpers ───────────────────────────────────────────────────────────────────

def now():
    return datetime.now(timezone.utc)


def to_object_id(val):
    """Safely convert a string to ObjectId, or return None."""
    if val is None:
        return None
    try:
        return bson.ObjectId(str(val))
    except Exception:
        return None


def ensure_university(platform_db, name: str, short_name: str) -> bson.ObjectId:
    """Get or create the WBU university document in the platform DB."""
    existing = platform_db.universities.find_one({"shortName": short_name})
    if existing:
        print(f"  [uni] Found existing university: {existing['_id']} ({short_name})")
        return existing["_id"]

    doc = {
        "name": name,
        "shortName": short_name,
        "country": "US",
        "status": "active",
        "createdAt": now(),
        "updatedAt": now(),
    }
    result = platform_db.universities.insert_one(doc)
    print(f"  [uni] Created university: {result.inserted_id} ({short_name})")
    return result.inserted_id


def ensure_program(platform_db, university_id: bson.ObjectId, title: str) -> bson.ObjectId:
    """Get or create the program document in the platform DB.
    
    NOTE: The programs collection has a unique index on {universityId, bundleUrl}.
    Since bundleUrl is null for all our programs, only one program per university
    can be created this way. We first try to find by title, then fall back to
    any existing program for that university.
    """
    # First: try exact match by title
    existing = platform_db.programs.find_one({
        "universityId": str(university_id),
        "title": title
    })
    if existing:
        print(f"  [prog] Found program: {existing['_id']} ({title})")
        return existing["_id"]

    # Second: try to find any existing program for this university
    # (the unique index prevents creating duplicates with bundleUrl=null)
    any_existing = platform_db.programs.find_one({"universityId": str(university_id)})
    if any_existing:
        # Update its title to match what we need, or just use it as-is
        print(f"  [prog] Using existing program: {any_existing['_id']} ('{any_existing.get('title')}' -> using for '{title}')")
        return any_existing["_id"]

    # Third: create new — first program for this university
    try:
        doc = {
            "title": title,
            "universityId": str(university_id),
            "status": "active",
            "createdAt": now(),
            "updatedAt": now(),
        }
        result = platform_db.programs.insert_one(doc)
        print(f"  [prog] Created program: {result.inserted_id} ({title})")
        return result.inserted_id
    except Exception as e:
        # Race condition or constraint violation — find what's there
        fallback = platform_db.programs.find_one({"universityId": str(university_id)})
        if fallback:
            print(f"  [prog] Fallback program: {fallback['_id']}")
            return fallback["_id"]
        raise e


def transform_item(src_item: dict) -> dict:
    """
    Transform a pipeline curriculum item to the platform's item schema.

    Pipeline item fields:
        title, slug, type, settings, content, attachments, position,
        quizConfig, assignmentConfig, questions, instructionalType,
        interactionLevel, estimatedDuration, learningOutcomes,
        classificationConfidence, _canvasId, _content_ref

    Platform item fields (from Mongoose model):
        _id, title, slug, type, settings, content, exerciseFiles,
        attachments, quizConfig, assignmentConfig, liveClassConfig,
        createdAt, updatedAt, questions
    """
    item = {
        "_id": bson.ObjectId(),
        "title": src_item.get("title", ""),
        "slug": src_item.get("slug", ""),
        "type": src_item.get("type", "Lesson"),
        "settings": {
            "isPublished":          src_item.get("settings", {}).get("isPublished", True),
            "isFreePreview":        src_item.get("settings", {}).get("isFreePreview", False),
            "isDownloadable":       src_item.get("settings", {}).get("isDownloadable", True),
            "isPrerequisite":       src_item.get("settings", {}).get("isPrerequisite", False),
            "requiresSubscription": src_item.get("settings", {}).get("requiresSubscription", False),
            "hiddenFromCurriculum": src_item.get("settings", {}).get("hiddenFromCurriculum", False),
        },
        "content":       src_item.get("content", ""),
        "exerciseFiles": [],
        "attachments":   src_item.get("attachments", []),
        "quizConfig":    src_item.get("quizConfig"),
        "assignmentConfig": src_item.get("assignmentConfig"),
        "liveClassConfig": {"isRecorded": False},
        "video":         src_item.get("video"),
        "videoUrl":      src_item.get("videoUrl"),
        "videoDuration": src_item.get("videoDuration"),
        "questions":     src_item.get("questions", []),
        "createdAt":     now(),
        "updatedAt":     now(),
        # ── Enrichment metadata (extra fields the platform can use) ──────
        "instructionalType":        src_item.get("instructionalType", ""),
        "interactionLevel":         src_item.get("interactionLevel", "passive"),
        "estimatedDuration":        src_item.get("estimatedDuration", 0),
        "learningOutcomes":         src_item.get("learningOutcomes", []),
        "classificationConfidence": src_item.get("classificationConfidence", 0.0),
    }
    return item


def transform_module(src_module: dict) -> dict:
    """Transform a pipeline module to the platform's module schema."""
    items = [transform_item(i) for i in src_module.get("items", [])]
    return {
        "_id":         bson.ObjectId(),
        "title":       src_module.get("title", ""),
        "summary":     src_module.get("summary", ""),
        "locked":      src_module.get("locked", False),
        "isVisible":   src_module.get("isVisible", True),
        "isPublished": src_module.get("isPublished", True),
        "settings":    src_module.get("settings", {}),
        "items":       items,
    }


def transform_course(
    src: dict,
    university_id: bson.ObjectId,
    program_id: bson.ObjectId,
    author_id: bson.ObjectId,
) -> dict:
    """
    Transform a pipeline course document to the platform's course schema.
    """
    curriculum = [transform_module(m) for m in src.get("curriculum", [])]

    # Count items by type for stats
    type_counts = {}
    for mod in curriculum:
        for item in mod["items"]:
            t = item["type"]
            type_counts[t] = type_counts.get(t, 0) + 1

    total_items = sum(type_counts.values())

    doc = {
        # ── Identity ──────────────────────────────────────────────────────
        "title":            src.get("title", ""),
        "slug":             src.get("slug", ""),
        "courseCode":       (src.get("courseCode", "") + "-SB") if "sandbox" in src.get("slug", "").lower() else src.get("courseCode", ""),
        "courseUrl":        src.get("courseUrl", ""),
        "department":       src.get("department", ""),

        # ── Relationships ─────────────────────────────────────────────────
        "university":       university_id,
        "universityId":     str(university_id),   # string copy for legacy queries
        "programId":        str(program_id),
        "authorId":         author_id,

        # ── Academic metadata ─────────────────────────────────────────────
        "credits":          src.get("credits", 3),
        "semester":         src.get("semester", ""),
        "academicYear":     src.get("academicYear", ""),
        "language":         src.get("language", "en"),
        "difficultyLevel":  src.get("difficultyLevel", "Intermediate"),

        # ── Descriptive ───────────────────────────────────────────────────
        "description":      src.get("description", ""),
        "shortDescription": src.get("shortDescription", ""),
        "featuredImage":    src.get("featuredImage", ""),
        "categories":       src.get("categories", []),
        "tags":             src.get("tags", []),

        # ── Pricing / access ──────────────────────────────────────────────
        "isPaid":           src.get("isPaid", False),
        "isPublic":         src.get("isPublic", True),
        "pricing":          src.get("pricing", {"amount": 0, "currency": "USD"}),

        # ── Status ────────────────────────────────────────────────────────
        "status":           src.get("status", "published"),
        "flags":            src.get("flags", {}),

        # ── Stats ─────────────────────────────────────────────────────────
        "stats": {
            "totalItems":       total_items,
            "totalModules":     len(curriculum),
            "itemsByType":      type_counts,
            "totalDuration":    sum(
                item.get("estimatedDuration", 0)
                for mod in curriculum
                for item in mod["items"]
            ),
        },
        "enrollmentCount":  src.get("enrollmentCount", 0),
        "applicantsCount":  src.get("applicantsCount", 0),
        "applications":     src.get("applications", []),

        # ── Curriculum ────────────────────────────────────────────────────
        "curriculum":       curriculum,

        # ── Misc ──────────────────────────────────────────────────────────
        "rubrics":          [],
        "prerequisites":    [],
        "coInstructors":    [],
        "assignedFaculty":  [],
        "contentDrip":      src.get("contentDrip", {"enabled": False}),
        "seo":              src.get("seo", {}),

        # ── Provenance ────────────────────────────────────────────────────
        "institution_code": src.get("institution_code", "WBU"),
        "source_pipeline":  "CourseOnboarding",
        "pipeline_course_id": str(src["_id"]),

        # ── Timestamps ────────────────────────────────────────────────────
        "createdAt":        src.get("createdAt", now()),
        "updatedAt":        now(),
        "__v":              0,
    }
    return doc


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Migrate courses to platform DB")
    parser.add_argument("--course-id", default=None,
                        help="Pipeline course ObjectId to migrate (default: WBU course)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview transformation without writing to platform DB")
    parser.add_argument("--all", action="store_true",
                        help="Migrate ALL courses from pipeline DB")
    parser.add_argument("--institution", default=None,
                        help="Institution code to filter by (e.g. SFC, WBU)")
    args = parser.parse_args()

    # ── Connect ───────────────────────────────────────────────────────────
    print("\nConnecting to databases...")
    pipeline_client = MongoClient(PIPELINE_URI)
    platform_client = MongoClient(PLATFORM_URI)

    pipeline_db = pipeline_client[PIPELINE_DB]
    platform_db = platform_client[PLATFORM_DB]

    print(f"  Pipeline: {PIPELINE_DB} @ cluster0.zyucmd7")
    print(f"  Platform: {PLATFORM_DB} @ cluster-staging.jsxinqf")

    # ── Known platform universities ───────────────────────────────────────
    # Map institution_code → (platform university ObjectId, name, short_name)
    SFC_UNI_ID = "69cb6cc4f1e9b0dda0713810"
    WBU_UNI_ID = None  # resolved dynamically from ensure_university

    INSTITUTION_UNI_MAP = {
        "SFC": ("69cb6cc4f1e9b0dda0713810", "St. Francis College", "SFC"),
    }

    # ── Select courses to migrate ─────────────────────────────────────────
    if args.all:
        query = {}
    elif args.course_id:
        query = {"_id": bson.ObjectId(args.course_id)}
    else:
        # Default: WBU course
        query = {"_id": bson.ObjectId("69f83af9d7082df5d618ff5d")}

    if args.institution:
        query["institution_code"] = args.institution.upper()

    courses = list(pipeline_db.courses.find(query))
    print(f"\nFound {len(courses)} course(s) to migrate.\n")
    # Fallback author lookup from platform
    first_user = platform_db.users.find_one({})
    default_author_oid = first_user["_id"] if first_user else bson.ObjectId()

    migrated = 0
    skipped  = 0
    errors   = 0

    # Cache university/program lookups to avoid repeated DB calls
    _uni_cache = {}
    _prog_cache = {}

    for src in courses:
        title = src.get("title", "?")
        slug  = src.get("slug", "?")
        institution_code = (src.get("institution_code") or "WBU").upper()
        print(f"Processing: {title}")
        print(f"  slug: {slug} | institution: {institution_code}")

        # Resolve author per course dynamically
        author_oid = to_object_id(src.get("authorId")) or to_object_id(os.getenv("DEFAULT_AUTHOR_ID")) or default_author_oid

        try:
            # ── Resolve university per course ─────────────────────────────
            if institution_code in INSTITUTION_UNI_MAP:
                uni_oid_str, uni_name, uni_short = INSTITUTION_UNI_MAP[institution_code]
                uni_id = bson.ObjectId(uni_oid_str)
                # Verify it exists (don't create SFC — it already exists)
                if not platform_db.universities.find_one({"_id": uni_id}):
                    print(f"  [WARN] University {uni_oid_str} not found in platform DB — using ensure_university")
                    uni_id = ensure_university(platform_db, uni_name, uni_short)
            else:
                # WBU or unknown — use ensure_university
                uni_name = "Wayland Baptist University"
                uni_short = institution_code
                cache_key = institution_code
                if cache_key not in _uni_cache:
                    _uni_cache[cache_key] = ensure_university(platform_db, uni_name, uni_short)
                uni_id = _uni_cache[cache_key]

            # ── Resolve program per course ────────────────────────────────
            prog_title = "General"
            if institution_code == "SFC":
                # Derive program from department
                dept = src.get("department", "")
                prog_title = dept or "General"
            elif institution_code == "WBU":
                prog_title = "Leadership & Management"

            prog_cache_key = f"{uni_id}:{prog_title}"
            if prog_cache_key not in _prog_cache:
                _prog_cache[prog_cache_key] = ensure_program(platform_db, uni_id, prog_title)
            prog_id = _prog_cache[prog_cache_key]

            doc = transform_course(src, uni_id, prog_id, author_oid)

            # Count items
            total = doc["stats"]["totalItems"]
            by_type = doc["stats"]["itemsByType"]
            print(f"  items: {total} total — " +
                  ", ".join(f"{t}:{n}" for t, n in sorted(by_type.items())))

            if args.dry_run:
                print("  [DRY RUN] Would upsert to platform DB")
                print(f"  [DRY RUN] university: {uni_id}, program: {prog_id}")
                skipped += 1
                continue

            # Upsert keyed on slug (idempotent)
            result = platform_db.courses.update_one(
                {"slug": slug},
                {"$set": doc},
                upsert=True
            )

            if result.upserted_id:
                print(f"  [OK] Inserted -> {result.upserted_id}")
            else:
                print(f"  [OK] Updated existing course (matched: {result.matched_count})")

            migrated += 1

        except Exception as e:
            import traceback
            print(f"  [ERROR] {e}")
            traceback.print_exc()
            errors += 1

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"Migration complete:")
    print(f"  Migrated : {migrated}")
    print(f"  Skipped  : {skipped} (dry-run)")
    print(f"  Errors   : {errors}")

    if not args.dry_run and migrated > 0:
        # Verify
        platform_course = platform_db.courses.find_one({"slug": courses[0]["slug"]})
        if platform_course:
            print(f"\nVerification — course in platform DB:")
            print(f"  _id    : {platform_course['_id']}")
            print(f"  title  : {platform_course['title']}")
            print(f"  modules: {len(platform_course.get('curriculum', []))}")
            total_items = sum(len(m.get('items', [])) for m in platform_course.get('curriculum', []))
            print(f"  items  : {total_items}")
            print(f"  status : {platform_course.get('status')}")
            print(f"  uni    : {platform_course.get('university')}")

    pipeline_client.close()
    platform_client.close()


if __name__ == "__main__":
    main()
