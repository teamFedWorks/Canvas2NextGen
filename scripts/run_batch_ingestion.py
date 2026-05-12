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
                    print(f"  [SKIP] No imsmanifest.xml in {entry.relative_to(uploads_root)} — skipping")
                continue
            courses.append((program_name, entry))
    return courses


def bar(label: str, width: int = 60) -> str:
    return f"\n{'─' * width}\n  {label}\n{'─' * width}"


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Batch course ingestion runner")
    ap.add_argument("--force",   action="store_true", help="Re-ingest even if course already exists")
    ap.add_argument("--dry-run", action="store_true", help="List courses without ingesting")
    ap.add_argument("--uploads", default=str(ROOT / "storage" / "uploads"),
                    help="Path to uploads root (default: storage/uploads)")
    args = ap.parse_args()

    uploads_root = Path(args.uploads)
    if not uploads_root.exists():
        print(f"[ERROR] Uploads directory not found: {uploads_root}")
        sys.exit(1)

    courses = discover_courses(uploads_root)
    if not courses:
        print("[ERROR] No valid course folders found.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Batch Ingestion — {len(courses)} course(s) found")
    print(f"  Uploads : {uploads_root}")
    print(f"  Outputs : {ROOT / 'storage' / 'outputs'}")
    print(f"  Force   : {args.force}")
    print(f"{'='*60}")

    if args.dry_run:
        print("\n[DRY RUN] Courses that would be ingested:\n")
        for i, (prog, folder) in enumerate(courses, 1):
            print(f"  {i:>2}. [{prog}]  {folder.name}")
        print(f"\nTotal: {len(courses)} course(s). Run without --dry-run to ingest.")
        return

    worker = IngestionWorker(
        s3_bucket=os.getenv("S3_CDN_BUCKET", "uhub-lms-bucket"),
        cdn_url=os.getenv("CDN_URL", "")
    )

    results = []
    total = len(courses)

    for idx, (program_name, course_folder) in enumerate(courses, 1):
        print(bar(f"[{idx}/{total}]  {course_folder.name}  ({program_name})"))

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
            print(f"  {tag}")
            if status == "success" and not deduped:
                print(f"  Course ID : {result.get('course_id')}")

        except Exception as exc:
            elapsed = time.time() - t0
            tag = f"CRASHED  — {exc}"
            results.append((course_folder.name, "crashed", False, tag))
            print(f"  {tag}")

    # ── final summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  BATCH COMPLETE — {total} course(s) processed")
    print(f"{'='*60}")

    success  = sum(1 for _, s, d, _ in results if s == "success" and not d)
    skipped  = sum(1 for _, s, d, _ in results if s == "success" and d)
    failed   = sum(1 for _, s, _, _ in results if s not in ("success",))

    print(f"  Ingested : {success}")
    print(f"  Skipped  : {skipped}  (already existed — use --force to re-ingest)")
    print(f"  Failed   : {failed}")
    print(f"\n  Validation reports → storage/outputs/\n")

    if failed:
        print("  Failed courses:")
        for name, status, _, tag in results:
            if status not in ("success",):
                print(f"    • {name}  →  {tag}")
        sys.exit(1)


if __name__ == "__main__":
    main()
