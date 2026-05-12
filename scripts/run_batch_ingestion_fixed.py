#!/usr/bin/env python3
"""
Batch Ingestion Runner
Processes every course folder under storage/uploads one by one,
runs the full pipeline, and generates validation reports in storage/outputs.

Usage:
  python scripts/run_batch_ingestion.py
  python scripts/run_batch_ingestion.py --force        # re-ingest even if already exists
  python scripts/run_batch_ingestion.py --dry-run      # list courses without ingesting
"""

import sys, os, argparse, time
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from worker.ingestion_worker import IngestionWorker

# ── helpers ───────────────────────────────────────────────────────────────────

SKIP_NAMES = {
    "BS Information Technology - Ingestion Report.html",
    "BS Information Technology - Ingestion Report.json",
    "BS Computer Science - Ingestion Report.html",
    "BS Computer Science - Ingestion Report.json",
    "Information Technology.pdf",
    "Computer Science report.pdf",
    "__pycache__",
}

def discover_courses(uploads_root: Path) -> list[tuple[str, Path]]:
    """
    Walk uploads_root and return (program_name, course_folder) for every
    course directory that contains an imsmanifest.xml.
    """
    courses = []
    for program_dir in sorted(uploads_root.iterdir()):
        if not program_dir.is_dir():
            continue
        program_name = program_dir.name
        for entry in sorted(program_dir.iterdir()):
            if entry.name in SKIP_NAMES:
                continue
            if not entry.is_dir():
                continue
            # Must contain imsmanifest.xml to be a valid Canvas export
            if not (entry / "imsmanifest.xml").exists():
                # Check one level deeper (some exports have a nested folder)
                nested = [d for d in entry.iterdir() if d.is_dir() and (d / "imsmanifest.xml").exists()]
                if nested:
                    for n in nested:
                        courses.append((program_name, n))
                else:
                    # Handle encoding issues when printing
                    try:
                        print(f"  [SKIP] No imsmanifest.xml in {entry.relative_to(uploads_root)} — skipping")
                    except UnicodeEncodeError:
                        print(f"  [SKIP] No imsmanifest.xml in [encoding issue] — skipping")
                    continue
            courses.append((program_name, entry))
    return courses


def safe_print(text):
    """Safely print text handling encoding issues"""
    try:
        print(text)
    except UnicodeEncodeError:
        # Replace problematic characters
        print(text.encode('ascii', 'replace').decode('ascii'))

def bar(label: str, width: int = 60) -> str:
    return f"\n{'─' * width}\n  {label}\n{'─' * width}"


# ── main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Batch course ingestion runner")
    ap.add_argument("--force",   action="store_true", help="Re-ingest even if course already exists")
    ap.add_argument("--dry-run", action="store_true", help="List courses without ingesting")
    ap.add_argument("--uploads", default=str(ROOT / "storage" / "uploads"),
                    help="Path to uploads root (default: storage/uploads)")
    args = ap.parse_args()

    uploads_root = Path(args.uploads)
    if not uploads_root.exists():
        safe_print(f"[ERROR] Uploads directory not found: {uploads_root}")
        sys.exit(1)

    courses = discover_courses(uploads_root)
    if not courses:
        safe_print("[ERROR] No valid course folders found.")
        sys.exit(1)

    try:
        safe_print(f"\n{'='*60}")
        safe_print(f"  Batch Ingestion — {len(courses)} course(s) found")
        safe_print(f"  Uploads : {uploads_root}")
        safe_print(f"  Outputs : {ROOT / 'storage' / 'outputs'}")
        safe_print(f"  Force   : {args.force}")
        safe_print(f"{'='*60}")
    except:
        safe_print(f"\n{'='*60}")
        safe_print(f"  Batch Ingestion — {len(courses)} course(s) found")
        safe_print(f"  Uploads : {uploads_root}")
        safe_print(f"  Outputs : {ROOT / 'storage' / 'outputs'}")
        safe_print(f"  Force   : {args.force}")
        safe_print(f"{'='*60}")

    if args.dry_run:
        try:
            safe_print("\n[DRY RUN] Courses that would be ingested:\n")
            for i, (prog, folder) in enumerate(courses, 1):
                try:
                    safe_print(f"  {i:>2}. [{prog}]  {folder.name}")
                except UnicodeEncodeError:
                    safe_print(f"  {i:>2}. [{prog}]  [encoding issue in folder name]")
            safe_print(f"\nTotal: {len(courses)} course(s). Run without --dry-run to ingest.")
        except:
            safe_print("\n[DRY RUN] Courses that would be ingested:\n")
            for i, (prog, folder) in enumerate(courses, 1):
                # Handle encoding issues in course names
                try:
                    safe_print(f"  {i:>2}. [{prog}]  {folder.name}")
                except UnicodeEncodeError:
                    safe_print(f"  {i:>2}. [{prog}]  [encoding issue in folder name]")
            safe_print(f"\nTotal: {len(courses)} course(s). Run without --dry-run to ingest.")
        return

    worker = IngestionWorker(
        s3_bucket=os.getenv("S3_CDN_BUCKET", "uhub-lms-bucket"),
        cdn_url=os.getenv("CDN_URL", "")
    )

    results = []
    total = len(courses)

    for idx, (program_name, course_folder) in enumerate(courses, 1):
        # Handle encoding issues in course names for display
        try:
            display_name = course_folder.name
        except UnicodeEncodeError:
            display_name = "[encoding issue in folder name]"
            
        try:
            safe_print(bar(f"[{idx}/{total}]  {display_name}  ({program_name})"))
        except:
            try:
                safe_print(bar(f"[{idx}/{total}]  [encoding issue]  ({program_name})"))
            except:
                safe_print(bar(f"[{idx}/{total}]  [issue]  ({program_name})"))

        t0 = time.time()
        try:
            result = worker.ingest(
                source_type="zip",
                payload={
                    "zip_path":      course_folder,          # folder — ZipAdapter handles it
                    "university_id": os.getenv("DEFAULT_UNIVERSITY_ID"),
                    "author_id":     os.getenv("DEFAULT_AUTHOR_ID"),
                    "program_name":  program_name,
                    "institution":   "SFC",   # local batch is always SFC
                    "force":         args.force,
                }
            )
            elapsed = time.time() - t0
            status  = result.get("status", "unknown")
            deduped = result.get("deduplicated", False)

            if status == "success" and deduped:
                tag = "SKIPPED (already exists)"
            elif status == "success":
                tag = f"SUCCESS  ({elapsed:.1f}s)"
            else:
                tag = f"FAILED   — {result.get('error','unknown error')}"

            results.append((course_folder.name, status, deduped, tag))
            try:
                safe_print(f"  {tag}")
            except UnicodeEncodeError:
                safe_print(f"  [{tag.encode('ascii', 'replace').decode('ascii')}]")
            
            if status == "success" and not deduped:
                try:
                    safe_print(f"  Course ID : {result.get('course_id')}")
                except UnicodeEncodeError:
                    safe_print(f"  Course ID : [encoding issue]")

        except Exception as exc:
            elapsed = time.time() - t0
            tag = f"CRASHED  — {exc}"
            results.append((course_folder.name, "crashed", False, tag))
            try:
                safe_print(f"  {tag}")
            except UnicodeEncodeError:
                safe_print(f"  [{tag.encode('ascii', 'replace').decode('ascii')}]")

    # ── final summary ─────────────────────────────────────────────────────────
    try:
        safe_print(f"\n{'='*60}")
        safe_print(f"  BATCH COMPLETE — {total} course(s) processed")
        safe_print(f"{'='*60}")
    except:
        safe_print(f"\n{'='*60}")
        safe_print(f"  BATCH COMPLETE — {total} course(s) processed")
        safe_print(f"{'='*60}")

    success  = sum(1 for _, s, d, _ in results if s == "success" and not d)
    skipped  = sum(1 for _, s, d, _ in results if s == "success" and d)
    failed   = sum(1 for _, s, _, _ in results if s not in ("success",))

    try:
        safe_print(f"  Ingested : {success}")
        safe_print(f"  Skipped  : {skipped}  (already existed — use --force to re-ingest)")
        safe_print(f"  Failed   : {failed}")
        safe_print(f"\n  Validation reports → storage/outputs/\n")
    except:
        safe_print(f"  Ingested : {success}")
        safe_print(f"  Skipped  : {skipped}  (already existed — use --force to re-ingest)")
        safe_print(f"  Failed   : {failed}")
        safe_print(f"\n  Validation reports → storage/outputs/\n")

    if failed:
        try:
            safe_print("  Failed courses:")
            for name, status, _, tag in results:
                if status not in ("success",):
                    try:
                        safe_print(f"    • {name}  →  {tag}")
                    except UnicodeEncodeError:
                        safe_print(f"    • [encoding issue]  →  [{tag.encode('ascii', 'replace').decode('ascii')}]")
        except:
            safe_print("  Failed courses:")
            for name, status, _, tag in results:
                if status not in ("success",):
                    try:
                        safe_print(f"    • {name}  →  {tag}")
                    except UnicodeEncodeError:
                        safe_print(f"    • [encoding issue]  →  [{tag.encode('ascii', 'replace').decode('ascii')}]")
        sys.exit(1)


if __name__ == "__main__":
    main()