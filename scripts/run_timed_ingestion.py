#!/usr/bin/env python3
"""
Timed Batch Ingestion Runner
Runs every course through the full pipeline and records exact wall-clock
onboarding duration per course.  Outputs:
  storage/outputs/timing_report.json   — machine-readable
  storage/outputs/timing_report.md     — human-readable Markdown table

Usage:
  python scripts/run_timed_ingestion.py
  python scripts/run_timed_ingestion.py --force
"""

import sys, os, argparse, time, json, math
from pathlib import Path
from datetime import datetime, timezone

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
    "BS_Computer_Science_-_Ingestion_Report.html",
    "BS_Computer_Science_-_Ingestion_Report.json",
    "BS Computer Science - Ingestion Report.html",
    "BS Computer Science - Ingestion Report.json",
    "Information Technology.pdf",
    "Computer Science report.pdf",
    "__pycache__",
}


def discover_courses(uploads_root: Path) -> list[tuple[str, Path]]:
    courses = []
    for entry in sorted(uploads_root.iterdir()):
        if entry.name in SKIP_NAMES or not entry.is_dir():
            continue
        if not (entry / "imsmanifest.xml").exists():
            found_any = False
            for sub in sorted(entry.iterdir()):
                if sub.name in SKIP_NAMES or not sub.is_dir():
                    continue
                if (sub / "imsmanifest.xml").exists():
                    courses.append((entry.name, sub))
                    found_any = True
                else:
                    nested = [d for d in sub.iterdir()
                              if d.is_dir() and (d / "imsmanifest.xml").exists()]
                    for n in nested:
                        courses.append((entry.name, n))
                        found_any = True
        else:
            courses.append((uploads_root.name, entry))
    return courses


def fmt_duration(seconds: float) -> str:
    """Return a human-friendly mm:ss.s string."""
    if seconds < 60:
        return f"{seconds:.1f} s"
    m = int(seconds // 60)
    s = seconds - m * 60
    return f"{m} min {s:.1f} s"


def safe_print(text: str):
    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"), flush=True)


def bar(label: str, width: int = 65) -> str:
    return f"\n{'─' * width}\n  {label}\n{'─' * width}"


# ── markdown report builder ───────────────────────────────────────────────────

def build_markdown(records: list[dict], batch_start: str, batch_end: str,
                   total_elapsed: float, outputs_dir: Path) -> str:
    lines = []

    lines.append("# Course Onboarding Timing Report\n")
    lines.append(f"**Batch Started  :** {batch_start}  ")
    lines.append(f"**Batch Finished :** {batch_end}  ")
    lines.append(f"**Total Wall-Clock Time :** {fmt_duration(total_elapsed)}  ")
    lines.append(f"**Total Courses :** {len(records)}  ")
    lines.append(f"**Report Saved   :** `{outputs_dir / 'timing_report.json'}`\n")
    lines.append("> Durations are **exact wall-clock seconds** measured by the")
    lines.append("> ingestion pipeline for each course (start → finish of `IngestionWorker.ingest()`).\n")
    lines.append("---\n")

    # ── group by program ──────────────────────────────────────────────────────
    programs: dict[str, list[dict]] = {}
    for r in records:
        programs.setdefault(r["program"], []).append(r)

    grand_ingested = grand_skipped = grand_failed = 0
    grand_total_s = 0.0

    for prog, prog_records in programs.items():
        lines.append(f"## {prog}\n")

        # table header
        lines.append("| # | Course | Status | Duration | Course ID |")
        lines.append("|---|--------|--------|----------|-----------|")

        prog_total_s = 0.0
        for i, r in enumerate(prog_records, 1):
            dur = fmt_duration(r["elapsed_seconds"]) if r["elapsed_seconds"] is not None else "—"
            status_text = {"success": "SUCCESS", "skipped": "SKIPPED", "failed": "FAILED", "crashed": "CRASHED"}.get(r["outcome"], "UNKNOWN")
            course_id = r.get("course_id") or "—"
            lines.append(
                f"| {i} | {r['course']} | {status_text} "
                f"| **{dur}** | `{course_id}` |"
            )
            if r["elapsed_seconds"]:
                prog_total_s += r["elapsed_seconds"]

        # program subtotal row
        lines.append(f"| | **Subtotal** | | **{fmt_duration(prog_total_s)}** | |")
        lines.append("")

        grand_total_s   += prog_total_s
        grand_ingested  += sum(1 for r in prog_records if r["outcome"] == "success")
        grand_skipped   += sum(1 for r in prog_records if r["outcome"] == "skipped")
        grand_failed    += sum(1 for r in prog_records if r["outcome"] in ("failed", "crashed"))

    # ── grand summary table ───────────────────────────────────────────────────
    lines.append("---\n")
    lines.append("## Grand Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Courses Processed | {len(records)} |")
    lines.append(f"| Ingested (new) | {grand_ingested} |")
    lines.append(f"| Skipped (already existed) | {grand_skipped} |")
    lines.append(f"| Failed / Crashed | {grand_failed} |")
    lines.append(f"| Total Onboarding Time | **{fmt_duration(grand_total_s)}** |")
    lines.append(f"| Average per Course | **{fmt_duration(grand_total_s / len(records) if records else 0)}** |")
    lines.append(f"| Fastest Course | **{min(records, key=lambda r: r['elapsed_seconds'] or 9e9)['course']}** ({fmt_duration(min(r['elapsed_seconds'] or 9e9 for r in records))}) |")
    lines.append(f"| Slowest Course | **{max(records, key=lambda r: r['elapsed_seconds'] or 0)['course']}** ({fmt_duration(max(r['elapsed_seconds'] or 0 for r in records))}) |")
    lines.append("")

    # ── per-program summary ───────────────────────────────────────────────────
    lines.append("## Per-Program Duration Summary\n")
    lines.append("| Program | Courses | Total Duration | Avg / Course |")
    lines.append("|---------|---------|----------------|-------------|")
    for prog, prog_records in programs.items():
        prog_s = sum(r["elapsed_seconds"] or 0 for r in prog_records)
        avg_s  = prog_s / len(prog_records) if prog_records else 0
        lines.append(f"| {prog} | {len(prog_records)} | **{fmt_duration(prog_s)}** | {fmt_duration(avg_s)} |")
    lines.append("")

    # ── report file paths ─────────────────────────────────────────────────────
    lines.append("## Report File Paths\n")
    lines.append("| Program | Ingestion Report (JSON) | Ingestion Report (HTML) |")
    lines.append("|---------|------------------------|------------------------|")

    report_paths = {
        "BS Information Technology": (
            str(ROOT / "storage" / "uploads" / "BS Information Technology" / "BS Information Technology - Ingestion Report.json"),
            str(ROOT / "storage" / "uploads" / "BS Information Technology" / "BS Information Technology - Ingestion Report.html"),
        ),
        "BS_Computer_Science": (
            str(ROOT / "storage" / "uploads" / "BS_Computer_Science" / "BS_Computer_Science_-_Ingestion_Report.json"),
            str(ROOT / "storage" / "uploads" / "BS_Computer_Science" / "BS_Computer_Science_-_Ingestion_Report.html"),
        ),
        "WBU": (
            "*(No report — Blackboard format)*",
            "*(No report — Blackboard format)*",
        ),
    }

    for prog in programs:
        j, h = report_paths.get(prog, ("—", "—"))
        lines.append(f"| {prog} | `{j}` | `{h}` |")
    lines.append("")
    lines.append(f"**This timing report:** `{outputs_dir / 'timing_report.json'}` · `{outputs_dir / 'timing_report.md'}`")

    return "\n".join(lines)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Timed batch course ingestion runner")
    ap.add_argument("--force",   action="store_true", help="Re-ingest even if course already exists")
    ap.add_argument("--uploads", default=str(ROOT / "storage" / "uploads"),
                    help="Path to uploads root (default: storage/uploads)")
    args = ap.parse_args()

    uploads_root = Path(args.uploads)
    outputs_dir  = ROOT / "storage" / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    if not uploads_root.exists():
        safe_print(f"[ERROR] Uploads directory not found: {uploads_root}")
        sys.exit(1)

    courses = discover_courses(uploads_root)
    if not courses:
        safe_print("[ERROR] No valid course folders found.")
        sys.exit(1)

    safe_print(f"\n{'='*65}")
    safe_print(f"  Timed Batch Ingestion — {len(courses)} course(s) found")
    safe_print(f"  Uploads : {uploads_root}")
    safe_print(f"  Outputs : {outputs_dir}")
    safe_print(f"  Force   : {args.force}")
    safe_print(f"{'='*65}\n")

    worker = IngestionWorker(
        s3_bucket=os.getenv("S3_CDN_BUCKET", "uhub-lms-bucket"),
        cdn_url=os.getenv("CDN_URL", "")
    )

    records: list[dict] = []
    total = len(courses)
    batch_start_dt = datetime.now(timezone.utc).isoformat()
    batch_t0 = time.time()

    for idx, (program_name, course_folder) in enumerate(courses, 1):
        display_name = course_folder.name
        safe_print(bar(f"[{idx}/{total}]  {display_name}  ({program_name})"))

        t0 = time.time()
        record: dict = {
            "program":         program_name,
            "course":          display_name,
            "folder_path":     str(course_folder),
            "started_at":      datetime.now(timezone.utc).isoformat(),
            "finished_at":     None,
            "elapsed_seconds": None,
            "elapsed_human":   None,
            "outcome":         None,
            "course_id":       None,
            "error":           None,
        }

        try:
            result = worker.ingest(
                source_type="zip",
                payload={
                    "zip_path":      course_folder,
                    "university_id": os.getenv("DEFAULT_UNIVERSITY_ID"),
                    "author_id":     os.getenv("DEFAULT_AUTHOR_ID"),
                    "program_name":  program_name,
                    "institution":   "SFC",
                    "force":         args.force,
                }
            )
            elapsed = time.time() - t0
            status  = result.get("status", "unknown")
            deduped = result.get("deduplicated", False)

            record["finished_at"]     = datetime.now(timezone.utc).isoformat()
            record["elapsed_seconds"] = round(elapsed, 3)
            record["elapsed_human"]   = fmt_duration(elapsed)
            record["course_id"]       = result.get("course_id")

            if status == "success" and deduped:
                record["outcome"] = "skipped"
                tag = f"SKIPPED (already exists)  [{fmt_duration(elapsed)}]"
            elif status == "success":
                record["outcome"] = "success"
                tag = f"SUCCESS  [{fmt_duration(elapsed)}]"
            else:
                record["outcome"] = "failed"
                record["error"]   = result.get("error", "unknown error")
                tag = f"FAILED   [{fmt_duration(elapsed)}]  — {record['error']}"

        except Exception as exc:
            elapsed = time.time() - t0
            record["finished_at"]     = datetime.now(timezone.utc).isoformat()
            record["elapsed_seconds"] = round(elapsed, 3)
            record["elapsed_human"]   = fmt_duration(elapsed)
            record["outcome"]         = "crashed"
            record["error"]           = str(exc)
            tag = f"CRASHED  [{fmt_duration(elapsed)}]  — {exc}"

        records.append(record)
        safe_print(f"  {tag}")
        if record.get("course_id"):
            safe_print(f"  Course ID : {record['course_id']}")

    # ── wrap up ───────────────────────────────────────────────────────────────
    total_elapsed  = time.time() - batch_t0
    batch_end_dt   = datetime.now(timezone.utc).isoformat()

    safe_print(f"\n{'='*65}")
    safe_print(f"  BATCH COMPLETE — {total} course(s) processed")
    safe_print(f"  Total time     : {fmt_duration(total_elapsed)}")
    safe_print(f"{'='*65}\n")

    ingested = sum(1 for r in records if r["outcome"] == "success")
    skipped  = sum(1 for r in records if r["outcome"] == "skipped")
    failed   = sum(1 for r in records if r["outcome"] in ("failed", "crashed"))
    safe_print(f"  Ingested : {ingested}")
    safe_print(f"  Skipped  : {skipped}  (use --force to re-ingest)")
    safe_print(f"  Failed   : {failed}")

    # ── write JSON report ─────────────────────────────────────────────────────
    json_payload = {
        "batch_started_at":  batch_start_dt,
        "batch_finished_at": batch_end_dt,
        "total_elapsed_seconds": round(total_elapsed, 3),
        "total_elapsed_human":   fmt_duration(total_elapsed),
        "total_courses": total,
        "ingested": ingested,
        "skipped":  skipped,
        "failed":   failed,
        "courses":  records,
    }
    json_path = outputs_dir / "timing_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, indent=2, ensure_ascii=False)
    safe_print(f"\n  JSON report → {json_path}")

    # ── write Markdown report ─────────────────────────────────────────────────
    md_content = build_markdown(records, batch_start_dt, batch_end_dt,
                                total_elapsed, outputs_dir)
    md_path = outputs_dir / "timing_report.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    safe_print(f"  MD  report → {md_path}\n")

    if failed:
        safe_print("  Failed courses:")
        for r in records:
            if r["outcome"] in ("failed", "crashed"):
                safe_print(f"    • {r['course']}  →  {r['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
