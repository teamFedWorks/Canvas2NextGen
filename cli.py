#!/usr/bin/env python3
"""
EduvateHub Course Onboarding CLI

A unified interface for managing course ingestions (ZIP, S3, Canvas) 
and starting the API server.
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add src to path for package imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Load .env
load_dotenv(".env")

from worker.ingestion_worker import IngestionWorker
from utils.s3_utils import S3Downloader
from observability.logger import get_logger

logger = get_logger(__name__)


def run_server():
    """Start the FastAPI server."""
    import uvicorn
    from api.main import app
    port = int(os.getenv("PORT", 5009))
    print(f"Starting EduvateHub Onboarding API on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)


def ingest_zip(args):
    """Process a local course ZIP."""
    zip_path = Path(args.path)
    if not zip_path.exists():
        print(f"[ERROR] ZIP file not found at {zip_path}")
        return

    worker = _get_worker()
    print(f"[INFO] Starting ingestion for {zip_path.name}...")
    result = worker.ingest(
        source_type="zip",
        payload={
            "zip_path":    zip_path,
            "university_id": args.uni,
            "author_id":   args.author,
            "institution": args.institution,
            "force":       args.force,
        }
    )
    _print_result(result)


def ingest_s3(args):
    """
    Ingest Canvas course packages from the structured S3 ingestion bucket.

    Supports any institution using the canonical key layout:
        Institutions/{Institution}/programs/{program}/courses/{code}.imscc
        Institutions/{Institution}/Programs/{program}/courses/{code}.zip

    Path segment casing is handled case-insensitively so both SFC
    (lowercase 'programs/') and WBU (capitalised 'Programs/') work
    with the same command.

    Examples
    --------
    # Dry-run — see everything that would be ingested
    python cli.py ingest-s3 --dry-run

    # Ingest all courses for a specific institution
    python cli.py ingest-s3 --institution SFC
    python cli.py ingest-s3 --institution WBU

    # Narrow to one program
    python cli.py ingest-s3 --institution WBU --program phd-program

    # Narrow to a single course (prefix match on course code)
    python cli.py ingest-s3 --institution SFC --program bs-computer-science --course it-2440

    # Force re-ingest (overwrite existing records)
    python cli.py ingest-s3 --institution SFC --force
    """
    import re, shutil, tempfile

    ingestion_bucket = os.getenv("S3_INGESTION_BUCKET")
    if not ingestion_bucket:
        print("❌  S3_INGESTION_BUCKET not set in .env")
        return

    uni_id    = args.uni    or os.getenv("DEFAULT_UNIVERSITY_ID")
    author_id = args.author or os.getenv("DEFAULT_AUTHOR_ID")

    if not uni_id:
        print("❌  No university ID supplied.  Pass --uni or set DEFAULT_UNIVERSITY_ID in .env")
        return

    # ── 1. Build S3 prefix ────────────────────────────────────────────────────
    # We use the institution name as-is (preserving case) because S3 keys are
    # case-sensitive.  The program segment is discovered via listing so we
    # don't need to know its casing upfront.
    prefix = f"Institutions/{args.institution}/"
    if args.program:
        # Try to find the actual casing by listing one level
        prefix += f"{args.program}/"   # will be refined below if needed
    if args.course:
        prefix += f"courses/{args.course}"

    downloader = S3Downloader(bucket=ingestion_bucket)
    all_keys = downloader.list_courses(
        prefix=prefix,
        extensions=('.zip', '.imscc'),
    )

    # If nothing found and a program was specified, try the other common casing
    if not all_keys and args.program and not args.course:
        alt_prefix = f"Institutions/{args.institution}/{args.program}/"
        all_keys = downloader.list_courses(
            prefix=alt_prefix,
            extensions=('.zip', '.imscc'),
        )
        if all_keys:
            prefix = alt_prefix

    if not all_keys:
        print(f"[INFO] No packages found under s3://{ingestion_bucket}/{prefix}")
        print(f"       Check the institution name and program slug are correct.")
        print(f"       Run with --dry-run to list what is available.")
        return

    # ── 2. Parse metadata from each key ──────────────────────────────────────
    # Canonical layout (case-insensitive on the 'programs' segment):
    #   Institutions/{inst}/{programs_segment}/{prog}/courses/{code}.{ext}
    # Both SFC  → Institutions/SFC/programs/bs-computer-science/courses/it-2440.imscc
    # and WBU   → Institutions/WBU/Programs/phd-program/courses/phd-course-shell.imscc
    # are matched by the same pattern.
    KEY_RE = re.compile(
        r'^Institutions/(?P<inst>[^/]+)/[Pp]rograms/(?P<prog>[^/]+)/[Cc]ourses/(?P<code>[^/]+)\.(zip|imscc)$',
        re.IGNORECASE,
    )

    packages = []
    unmatched = []
    for key in all_keys:
        m = KEY_RE.match(key)
        if not m:
            unmatched.append(key)
            continue
        packages.append({
            "key":         key,
            "institution": m.group("inst"),
            "program":     m.group("prog"),
            "course_code": m.group("code"),
        })

    # ── 3. Print plan ─────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"  S3 Ingestion — {len(packages)} package(s) found")
    print(f"  Bucket      : {ingestion_bucket}")
    print(f"  Prefix      : {prefix}")
    print(f"  University  : {uni_id}")
    print(f"  Author      : {author_id}")
    print(f"  Force       : {args.force}")
    print(f"{'='*64}")

    if unmatched:
        print(f"\n  [WARN] {len(unmatched)} key(s) did not match the expected path layout — skipped:")
        for k in unmatched:
            print(f"    • {k}")

    if args.dry_run:
        print("\n[DRY RUN] Packages that would be ingested:\n")
        for i, p in enumerate(packages, 1):
            print(f"  {i:>2}. [{p['institution']} / {p['program']}]  {p['course_code']}")
            print(f"       s3://{ingestion_bucket}/{p['key']}")
        print(f"\nTotal: {len(packages)} package(s).  Run without --dry-run to ingest.")
        return

    # ── 4. Ingest sequentially ────────────────────────────────────────────────
    worker  = _get_worker()
    results = []

    for idx, pkg in enumerate(packages, 1):
        key          = pkg["key"]
        program_name = pkg["program"].replace("-", " ").title()
        course_code  = pkg["course_code"].upper()

        print(f"\n{'─'*64}")
        print(f"  [{idx}/{len(packages)}]  {course_code}  ({program_name})")
        print(f"{'─'*64}")

        tmp_dir = Path(tempfile.mkdtemp(prefix="s3_ingest_"))
        try:
            local_path = downloader.download(key, tmp_dir)

            result = worker.ingest(
                source_type="zip",
                payload={
                    "zip_path":      local_path,
                    "university_id": uni_id,
                    "author_id":     author_id,
                    "program_name":  program_name,
                    "institution":   pkg["institution"],   # e.g. "SFC" or "WBU"
                    "force":         args.force,
                },
            )

            status  = result.get("status", "unknown")
            deduped = result.get("deduplicated", False)

            if status == "success" and deduped:
                tag = "SKIPPED  (already exists — use --force to re-ingest)"
            elif status == "success":
                tag = f"SUCCESS  → course_id={result.get('course_id')}"
            else:
                tag = f"FAILED   — {result.get('error', 'unknown error')}"

            print(f"  {tag}")
            results.append((course_code, status, deduped))

        except Exception as exc:
            print(f"  CRASHED  — {exc}")
            results.append((course_code, "crashed", False))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── 5. Summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"  BATCH COMPLETE — {len(packages)} package(s) processed")
    print(f"{'='*64}")
    ingested    = sum(1 for _, s, d in results if s == "success" and not d)
    skipped_dup = sum(1 for _, s, d in results if s == "success" and d)
    failed      = sum(1 for _, s, _ in results if s not in ("success",))
    print(f"  Ingested : {ingested}")
    print(f"  Skipped  : {skipped_dup}  (duplicates)")
    print(f"  Failed   : {failed}")
    print(f"\n  Validation reports → storage/outputs/\n")
    if failed:
        print("  Failed courses:")
        for code, status, _ in results:
            if status not in ("success",):
                print(f"    • {code}")
        sys.exit(1)


def ingest_canvas(args):
    """Trigger a Canvas API migration."""
    worker = _get_worker()
    print(f"[INFO] Triggering Canvas API migration for Course ID: {args.course_id}...")
    result = worker.ingest(
        source_type="canvas",
        payload={
            "course_id": args.course_id,
            "university_id": args.uni,
            "author_id": args.author,
            "force": args.force
        }
    )
    _print_result(result)


def _get_worker():
    """Shared worker initialization."""
    s_bucket = os.getenv("S3_CDN_BUCKET")
    c_url = os.getenv("CDN_URL")
    return IngestionWorker(s3_bucket=s_bucket, cdn_url=c_url)


def _print_result(result):
    """Standardized result printing."""
    if result.get("status") == "success":
        print(f"[SUCCESS] Ingestion Successful!")
        print(f"   Course ID: {result.get('course_id')}")
        if result.get("deduplicated"):
            print("   (Course already existed, skipped re-import)")
    else:
        print(f"[ERROR] Ingestion Failed: {result.get('error')}")


def main():
    parser = argparse.ArgumentParser(description="EduvateHub Onboarding CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Server command
    subparsers.add_parser("server", help="Start the FastAPI server")

    # Ingest Zip command
    zip_parser = subparsers.add_parser("ingest-zip", help="Ingest a local ZIP")
    zip_parser.add_argument("--path", required=True, help="Path to local .zip/.imscc file")
    zip_parser.add_argument("--uni", default=os.getenv("DEFAULT_UNIVERSITY_ID", "default_uni"), help="University ID")
    zip_parser.add_argument("--author", default=os.getenv("DEFAULT_AUTHOR_ID", "default_author"), help="Author ID")
    zip_parser.add_argument("--institution", default="SFC",
                            help="Institution code for S3 folder and report grouping (default: SFC)")
    zip_parser.add_argument("--force", action="store_true", help="Force re-import")

    # Ingest S3 command — institution-agnostic, works for SFC, WBU, and any future institution
    s3_parser = subparsers.add_parser(
        "ingest-s3",
        help="Ingest Canvas packages from S3 (Institutions/{inst}/programs/{prog}/courses/)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Ingest Canvas packages from the structured S3 ingestion bucket.\n\n"
            "Supported key layouts (case-insensitive on 'programs'/'Programs'):\n"
            "  Institutions/{inst}/programs/{prog}/courses/{code}.imscc\n"
            "  Institutions/{inst}/Programs/{prog}/courses/{code}.zip\n\n"
            "Examples:\n"
            "  # Dry-run — see what would be ingested\n"
            "  python cli.py ingest-s3 --dry-run\n\n"
            "  # Ingest all SFC courses\n"
            "  python cli.py ingest-s3 --institution SFC\n\n"
            "  # Ingest all WBU courses\n"
            "  python cli.py ingest-s3 --institution WBU --uni $WBU_UNIVERSITY_ID\n\n"
            "  # Narrow to one program\n"
            "  python cli.py ingest-s3 --institution WBU --program phd-program\n\n"
            "  # Single course\n"
            "  python cli.py ingest-s3 --institution SFC --program bs-computer-science --course it-2440\n\n"
            "  # Force re-ingest\n"
            "  python cli.py ingest-s3 --institution SFC --force"
        ),
    )
    s3_parser.add_argument(
        "--institution", default=None,
        help="Institution folder name in S3, e.g. SFC or WBU.  "
             "Omit to scan all institutions (use with --dry-run first)."
    )
    s3_parser.add_argument(
        "--program", default=None,
        help="Limit to one program slug, e.g. bs-computer-science or phd-program"
    )
    s3_parser.add_argument(
        "--course", default=None,
        help="Limit to one course code prefix, e.g. it-2440 or phd-course-shell"
    )
    s3_parser.add_argument(
        "--uni", default=None,
        help="MongoDB university ID.  Defaults to DEFAULT_UNIVERSITY_ID in .env"
    )
    s3_parser.add_argument(
        "--author", default=None,
        help="MongoDB author ID.  Defaults to DEFAULT_AUTHOR_ID in .env"
    )
    s3_parser.add_argument("--force",   action="store_true", help="Re-ingest even if course exists")
    s3_parser.add_argument("--dry-run", action="store_true", help="List packages without ingesting")

    # Ingest Canvas command
    canvas_parser = subparsers.add_parser("ingest-canvas", help="Ingest from Canvas API")
    canvas_parser.add_argument("--course-id", required=True, help="Canvas Course ID")
    canvas_parser.add_argument("--uni", required=True, help="University ID")
    canvas_parser.add_argument("--author", required=True, help="Author ID")
    canvas_parser.add_argument("--force", action="store_true", help="Force re-import")

    args = parser.parse_args()

    if args.command == "server":
        run_server()
    elif args.command == "ingest-zip":
        ingest_zip(args)
    elif args.command == "ingest-s3":
        ingest_s3(args)
    elif args.command == "ingest-canvas":
        ingest_canvas(args)


if __name__ == "__main__":
    main()
