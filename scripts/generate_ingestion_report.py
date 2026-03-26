#!/usr/bin/env python3
"""
Consolidated Ingestion Report Generator

Produces a full ingestion audit covering:
  - Asset-level status (pass / fail / retry) for all file types
  - Deck-level (module) status derived from imsmanifest.xml
  - Course-level structural gaps
  - Missing thumbnail detection (PPT/PPTX only)

Usage:
  python scripts/generate_ingestion_report.py                  # all courses
  python scripts/generate_ingestion_report.py --course "01 - PHI-1114 Logic and Argumentation"
  python scripts/generate_ingestion_report.py --output storage/my_report.json
  python scripts/generate_ingestion_report.py --no-html        # skip HTML output
"""

import sys
import os
import json
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT_DIR    = Path("storage/uploads")
OUTPUT_JSON = Path("storage/ingestion_report.json")
OUTPUT_HTML = Path("storage/ingestion_report.html")

# All extensions are lowercased for comparison
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".jfif", ".gif", ".svg", ".webp", ".bmp", ".ico"}
VIDEO_EXTS = {".mp4", ".webm", ".mov", ".avi", ".mkv", ".ogv", ".m4v", ".mp3", ".wav", ".ogg", ".m4a"}
PPT_EXTS   = {".pptx", ".ppt"}
DOC_EXTS   = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".txt"}
XML_EXTS   = {".xml"}
QTI_EXTS   = {".qti"}                          # Canvas quiz exports
HTML_EXTS  = {".html", ".htm"}
OTHER_EXTS = {".zip", ".csv", ".ipynb", ".json", ".js", ".css", ".py", ".rb"}

THUMBNAIL_KEYWORDS = {"thumbnail", "thumb", "cover", "preview", "poster"}

# IMS-CC namespaces
NS = {
    "imscc": "http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1",
    "imsmd": "http://ltsc.ieee.org/xsd/imsccv1p1/LOM/manifest",
}

REQUIRED_DIRS  = ["course_settings"]
REQUIRED_FILES = ["imsmanifest.xml", "course_settings/course_settings.xml"]
MIN_MODULE_COUNT = 2

# Directories to always exclude from asset scanning (pipeline output, not source)
EXCLUDE_DIRS = {"tutor_lms_output", ".git", "__pycache__", "lms_output"}

# ---------------------------------------------------------------------------
# Asset helpers
# ---------------------------------------------------------------------------

def _asset_status(path: Path) -> str:
    """
    pass  – file readable and meets minimum size
    retry – suspiciously small for its type
    fail  – empty or unreadable
    """
    try:
        size = path.stat().st_size
    except OSError:
        return "fail"

    if size == 0:
        return "fail"

    ext = path.suffix.lower()  # normalize case for all comparisons
    if ext in (XML_EXTS | QTI_EXTS | PPT_EXTS | DOC_EXTS) and size < 512:
        return "retry"
    if ext in (IMAGE_EXTS | VIDEO_EXTS) and size < 100:
        return "retry"
    if ext in HTML_EXTS and size < 50:
        return "retry"
    if ext == "" and size < 1000:   # extensionless media — flag if suspiciously tiny
        return "retry"

    return "pass"


def _classify_ext(ext: str) -> Optional[str]:
    """Return asset category key or None if untracked."""
    e = ext.lower()
    if e in XML_EXTS:   return "xml"
    if e in QTI_EXTS:   return "qti"
    if e in PPT_EXTS:   return "ppt"
    if e in IMAGE_EXTS: return "images"
    if e in DOC_EXTS:   return "documents"
    if e in VIDEO_EXTS: return "videos"
    if e in HTML_EXTS:  return "html"
    if e in OTHER_EXTS: return "other"
    if e == "":         return "media"   # extensionless Canvas media recordings
    return None


def _empty_bucket() -> Dict[str, Any]:
    return {"total": 0, "pass": 0, "fail": 0, "retry": 0, "files": []}


# ---------------------------------------------------------------------------
# Deep-scan: find the true course root (where imsmanifest.xml lives)
# ---------------------------------------------------------------------------

def _find_course_root(course_dir: Path) -> tuple[Path, Optional[str]]:
    """
    Walk the directory tree to find the folder that contains imsmanifest.xml.

    Returns (true_root, nesting_note):
      - true_root      : the Path where imsmanifest.xml was found
      - nesting_note   : human-readable note if content was nested, else None

    Strategy:
      1. Check the given directory directly (fast path, most common case).
      2. If not found, search one level deep (single wrapper folder).
      3. If still not found, do a full recursive search (any depth).
      4. If multiple manifests exist, prefer the shallowest one that is NOT
         inside a 'quiz' or 'non_cc_assessments' subfolder.
    """
    # Fast path — manifest at root
    if (course_dir / "imsmanifest.xml").exists():
        return course_dir, None

    # Collect all manifests recursively, excluding known sub-package dirs
    candidates: List[Path] = []
    for p in course_dir.rglob("imsmanifest.xml"):
        # Skip manifests inside quiz / non_cc_assessments sub-packages
        parts = set(p.relative_to(course_dir).parts[:-1])  # parent path parts
        if parts & {"quiz", "non_cc_assessments"}:
            continue
        candidates.append(p.parent)

    if not candidates:
        return course_dir, None  # nothing found — return original, gaps will flag it

    # Pick shallowest candidate
    candidates.sort(key=lambda p: len(p.relative_to(course_dir).parts))
    true_root = candidates[0]
    rel = true_root.relative_to(course_dir)
    note = f"Content nested inside subfolder: '{rel}'" if str(rel) != "." else None
    return true_root, note


# ---------------------------------------------------------------------------
# Manifest / deck parsing  (only reads the top-level manifest)
# ---------------------------------------------------------------------------

def _parse_manifest(course_dir: Path) -> Dict[str, Any]:
    manifest_path = course_dir / "imsmanifest.xml"
    if not manifest_path.exists():
        return {"modules": [], "deck_count": 0, "parse_error": "imsmanifest.xml not found"}

    try:
        tree = ET.parse(manifest_path)
        root = tree.getroot()
    except ET.ParseError as exc:
        return {"modules": [], "deck_count": 0, "parse_error": f"XML parse error: {exc}"}

    def _tag(el: ET.Element) -> str:
        return el.tag.split("}")[-1] if "}" in el.tag else el.tag

    modules: List[Dict[str, Any]] = []

    for orgs in root.iter():
        if _tag(orgs) != "organizations":
            continue
        for org in orgs:
            if _tag(org) != "organization":
                continue
            for item in org:
                if _tag(item) != "item":
                    continue
                title_el = next((c for c in item if _tag(c) == "title"), None)
                title = (title_el.text or "").strip() if title_el is not None else item.get("identifier", "Untitled")
                sub_items = [
                    {
                        "identifier": sub.get("identifier", ""),
                        "title": ((next((c for c in sub if _tag(c) == "title"), None) or ET.Element("x")).text or "").strip(),
                        "identifierref": sub.get("identifierref", ""),
                    }
                    for sub in item if _tag(sub) == "item"
                ]
                modules.append({
                    "identifier": item.get("identifier", ""),
                    "title": title,
                    "items": sub_items,
                    "item_count": len(sub_items),
                })

    return {"modules": modules, "deck_count": len(modules), "parse_error": None}


# ---------------------------------------------------------------------------
# Thumbnail detection  (PPT/PPTX only — slides are the ones needing covers)
# ---------------------------------------------------------------------------

def _find_missing_thumbnails(course_dir: Path, assets: Dict[str, Any]) -> List[str]:
    image_stems = {
        Path(f["name"]).stem.lower()
        for f in assets.get("images", {}).get("files", [])
    }
    has_any_thumbnail_image = any(
        any(kw in f["name"].lower() for kw in THUMBNAIL_KEYWORDS)
        for f in assets.get("images", {}).get("files", [])
    )

    missing: List[str] = []
    for finfo in assets.get("ppt", {}).get("files", []):
        stem = Path(finfo["name"]).stem.lower()
        if stem not in image_stems and not has_any_thumbnail_image:
            missing.append(finfo["name"])

    return missing


# ---------------------------------------------------------------------------
# Course analysis
# ---------------------------------------------------------------------------

def analyze_course(course_dir: Path) -> Dict[str, Any]:
    course_name = course_dir.name

    # --- Deep-scan: resolve the true course root ---
    true_root, nesting_note = _find_course_root(course_dir)

    # All tracked categories
    assets: Dict[str, Any] = {
        "xml":       _empty_bucket(),
        "qti":       _empty_bucket(),
        "html":      _empty_bucket(),
        "ppt":       _empty_bucket(),
        "images":    _empty_bucket(),
        "documents": _empty_bucket(),
        "videos":    _empty_bucket(),
        "other":     _empty_bucket(),
        "media":     _empty_bucket(),
    }

    # Scan from true_root, skipping excluded dirs
    for path in true_root.rglob("*"):
        if not path.is_file():
            continue
        # Skip any file inside an excluded directory
        if any(part in EXCLUDE_DIRS for part in path.relative_to(true_root).parts):
            continue
        cat = _classify_ext(path.suffix)
        if cat is None:
            continue
        status = _asset_status(path)
        size = 0
        try:
            size = path.stat().st_size
        except OSError:
            pass
        bucket = assets[cat]
        bucket["total"] += 1
        bucket[status] += 1
        bucket["files"].append({
            "name": path.name,
            "path": str(path.relative_to(true_root)),
            "size_bytes": size,
            "status": status,
        })

    # Manifest / deck info (always from true_root)
    manifest_info = _parse_manifest(true_root)

    deck_statuses: List[Dict[str, Any]] = [
        {
            "identifier": mod["identifier"],
            "title": mod["title"],
            "item_count": mod["item_count"],
            "status": "pass" if mod["item_count"] > 0 else "warn",
        }
        for mod in manifest_info["modules"]
    ]

    # Structural gaps
    gaps: List[str] = []

    if nesting_note:
        gaps.append(f"Structure note: {nesting_note}")

    if manifest_info["parse_error"]:
        gaps.append(f"Manifest error: {manifest_info['parse_error']}")

    for req_dir in REQUIRED_DIRS:
        if not (true_root / req_dir).is_dir():
            gaps.append(f"Missing required directory: {req_dir}")

    for req_file in REQUIRED_FILES:
        if not (true_root / req_file).exists():
            gaps.append(f"Missing required file: {req_file}")

    deck_count = manifest_info["deck_count"]
    if deck_count == 0:
        gaps.append(f"No modules found in manifest (expected >= 1)")

    for mod in manifest_info["modules"]:
        if mod["item_count"] == 0:
            gaps.append(f"Empty module (no items): '{mod['title']}'")

    total_assets = sum(assets[k]["total"] for k in assets)
    if total_assets == 0:
        gaps.append("No tracked assets found in course directory")

    missing_thumbnails = _find_missing_thumbnails(true_root, assets)

    return {
        "name": course_name,
        "true_root": str(true_root.relative_to(course_dir)) if true_root != course_dir else ".",
        "assets": assets,
        "deck_statuses": deck_statuses,
        "deck_count": deck_count,
        "manifest_error": manifest_info["parse_error"],
        "gaps": gaps,
        "missing_thumbnails": missing_thumbnails,
        "total_assets": total_assets,
        "has_issues": bool(gaps or missing_thumbnails),
    }


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------

ASSET_CATS = ["xml", "qti", "html", "ppt", "images", "documents", "videos", "other", "media"]


def generate_report(root_dir: Path, course_filter: Optional[str] = None) -> Dict[str, Any]:
    if not root_dir.exists():
        sys.exit(f"ERROR: uploads directory not found: {root_dir}")

    course_dirs = sorted(d for d in root_dir.iterdir() if d.is_dir())

    if course_filter:
        course_dirs = [d for d in course_dirs if course_filter.lower() in d.name.lower()]
        if not course_dirs:
            sys.exit(f"ERROR: No course matching '{course_filter}' found in {root_dir}")

    asset_summary: Dict[str, Any] = {
        cat: {"total": 0, "pass": 0, "fail": 0, "retry": 0}
        for cat in ASSET_CATS
    }

    courses: List[Dict[str, Any]] = []
    total_assets = 0
    courses_with_issues = 0
    missing_thumbnails_total = 0

    for course_dir in course_dirs:
        data = analyze_course(course_dir)
        courses.append(data)
        total_assets += data["total_assets"]
        if data["has_issues"]:
            courses_with_issues += 1
        missing_thumbnails_total += len(data["missing_thumbnails"])
        for cat in ASSET_CATS:
            for stat in ("total", "pass", "fail", "retry"):
                asset_summary[cat][stat] += data["assets"][cat][stat]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root_dir": str(root_dir),
        "course_filter": course_filter,
        "summary": {
            "total_courses": len(courses),
            "total_assets": total_assets,
            "courses_successful": len(courses) - courses_with_issues,
            "courses_remaining": courses_with_issues,
            "courses_with_issues": courses_with_issues,
            "missing_thumbnails_total": missing_thumbnails_total,
        },
        "asset_summary": asset_summary,
        "courses": courses,
    }


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_report(report: Dict[str, Any]) -> None:
    W = 80
    print("=" * W)
    print("CONSOLIDATED INGESTION REPORT")
    print("=" * W)
    print(f"Generated : {report['generated_at']}")
    print(f"Source    : {report['root_dir']}")
    if report.get("course_filter"):
        print(f"Filter    : {report['course_filter']}")
    print()

    s = report["summary"]
    print("SUMMARY")
    print("-" * 40)
    print(f"  Total courses        : {s['total_courses']}")
    print(f"  Total assets         : {s['total_assets']}")
    print(f"  Courses with issues  : {s['courses_with_issues']}")
    print(f"  Missing thumbnails   : {s['missing_thumbnails_total']}")
    print()

    print("ASSET-LEVEL STATUS (all courses)")
    print(f"  {'Type':<12} {'Total':>7} {'Pass':>7} {'Fail':>7} {'Retry':>7}")
    print("  " + "-" * 42)
    for cat, counts in report["asset_summary"].items():
        if counts["total"] == 0:
            continue
        print(f"  {cat:<12} {counts['total']:>7} {counts['pass']:>7} {counts['fail']:>7} {counts['retry']:>7}")
    print()

    print("COURSE-LEVEL DETAILS")
    print("=" * W)
    for course in report["courses"]:
        icon = "[X]" if course["has_issues"] else "[OK]"
        print(f"\n{icon} {course['name']}")
        print(f"    Total files: {course['total_assets']}  |  Modules: {course['deck_count']}")

        if course["deck_statuses"]:
            print("    Deck / Module Status:")
            for deck in course["deck_statuses"]:
                dk = "OK" if deck["status"] == "pass" else "!!"
                print(f"      [{dk}] {deck['title']}  ({deck['item_count']} items)")

        print("    Asset Breakdown:")
        for cat, counts in course["assets"].items():
            if counts["total"] == 0:
                continue
            fail_str  = f"  fail={counts['fail']}"   if counts["fail"]  else ""
            retry_str = f"  retry={counts['retry']}"  if counts["retry"] else ""
            print(f"      {cat:<12} total={counts['total']}  pass={counts['pass']}{fail_str}{retry_str}")

        if course["gaps"]:
            print("    Gaps:")
            for gap in course["gaps"]:
                print(f"      !! {gap}")

        if course["missing_thumbnails"]:
            print("    Missing Thumbnails (PPT/PPTX without cover image):")
            for t in course["missing_thumbnails"]:
                print(f"      !! {t}")

    print()
    print("=" * W)


# ---------------------------------------------------------------------------
# HTML output  (with PDF download via window.print())
# ---------------------------------------------------------------------------

def _badge(status: str) -> str:
    colors = {
        "pass":  ("#e8f5e9", "#2e7d32"),
        "fail":  ("#ffebee", "#c62828"),
        "retry": ("#fff3e0", "#e65100"),
        "warn":  ("#fffde7", "#f57f17"),
    }
    bg, fg = colors.get(status, ("#f5f5f5", "#555"))
    return (
        f'<span style="background:{bg};color:{fg};border:1px solid {fg};'
        f'padding:2px 10px;border-radius:12px;font-size:0.78em;font-weight:600">'
        f'{status.upper()}</span>'
    )


def generate_html(report: Dict[str, Any]) -> str:
    s = report["summary"]

    # ---- Global asset summary table rows ----
    asset_rows = ""
    for cat, counts in report["asset_summary"].items():
        if counts["total"] == 0:
            continue
        fail_style  = ' style="color:#c62828;font-weight:700"' if counts["fail"]  else ""
        retry_style = ' style="color:#e65100;font-weight:600"' if counts["retry"] else ""
        pct_pass = round(counts["pass"] / counts["total"] * 100) if counts["total"] else 0
        bar = (
            f'<div style="background:#e0e0e0;border-radius:4px;height:10px;width:120px;display:inline-block;vertical-align:middle">'
            f'<div style="background:#2e7d32;width:{pct_pass}%;height:100%;border-radius:4px"></div></div>'
            f' <small>{pct_pass}%</small>'
        )
        asset_rows += (
            f"<tr><td><strong>{cat}</strong></td><td>{counts['total']}</td>"
            f"<td style='color:#2e7d32'>{counts['pass']}</td>"
            f"<td{fail_style}>{counts['fail']}</td>"
            f"<td{retry_style}>{counts['retry']}</td>"
            f"<td>{bar}</td></tr>\n"
        )

    # ---- Per-course sections ----
    course_sections = ""
    for course in report["courses"]:
        border_color = "#e65100" if course["has_issues"] else "#2e7d32"
        status_label = _badge("fail" if course["has_issues"] else "pass")

        # Deck table
        deck_rows = "".join(
            f"<tr><td>{d['title']}</td><td style='text-align:center'>{d['item_count']}</td>"
            f"<td>{_badge(d['status'])}</td></tr>"
            for d in course["deck_statuses"]
        )
        deck_table = (
            f"<table><thead><tr><th>Module / Deck</th><th>Items</th><th>Status</th></tr></thead>"
            f"<tbody>{deck_rows}</tbody></table>"
            if deck_rows else "<p class='muted'>No modules found in manifest.</p>"
        )

        # Asset breakdown table
        a_rows = ""
        for cat, counts in course["assets"].items():
            if counts["total"] == 0:
                continue
            pct = round(counts["pass"] / counts["total"] * 100) if counts["total"] else 0
            bar = (
                f'<div style="background:#e0e0e0;border-radius:4px;height:8px;width:100px;display:inline-block;vertical-align:middle">'
                f'<div style="background:#2e7d32;width:{pct}%;height:100%;border-radius:4px"></div></div>'
                f' <small>{pct}%</small>'
            )
            fail_s  = f' style="color:#c62828;font-weight:700"' if counts["fail"]  else ""
            retry_s = f' style="color:#e65100"'                  if counts["retry"] else ""
            a_rows += (
                f"<tr><td>{cat}</td><td>{counts['total']}</td>"
                f"<td style='color:#2e7d32'>{counts['pass']}</td>"
                f"<td{fail_s}>{counts['fail']}</td>"
                f"<td{retry_s}>{counts['retry']}</td>"
                f"<td>{bar}</td></tr>"
            )
        asset_table = (
            f"<table><thead><tr><th>Type</th><th>Total</th><th>Pass</th><th>Fail</th><th>Retry</th><th>Pass Rate</th></tr></thead>"
            f"<tbody>{a_rows}</tbody></table>"
            if a_rows else "<p class='muted'>No tracked assets.</p>"
        )

        # Gaps
        gaps_html = (
            "<ul class='gap-list'>" + "".join(f"<li>{g}</li>" for g in course["gaps"]) + "</ul>"
            if course["gaps"] else "<p class='muted'>None</p>"
        )

        # Missing thumbnails
        thumbs_html = ""
        if course["missing_thumbnails"]:
            thumb_list = "".join(f"<li>{t}</li>" for t in course["missing_thumbnails"])
            thumbs_html = f"""
            <ul class='gap-list'>{thumb_list}</ul>
            <div class="action-box">
              <strong>Action Required</strong>
              <p>The following slide decks are missing a cover image. Please contact the course author from SFC and request a thumbnail (cover slide screenshot saved as a <code>.png</code>) for each file listed above.</p>
              <p>Send to: <strong>Course Author / SFC Content Team</strong></p>
              <ul class="action-steps">
                <li>Reply to the original course submission email referencing course <strong>{course['name']}</strong></li>
                <li>List the PPT/PPTX files above and ask for a <code>cover_&lt;filename&gt;.png</code> for each</li>
                <li>Once received, place the images in the course's <code>web_resources/</code> folder and re-run ingestion</li>
              </ul>
            </div>"""
        else:
            thumbs_html = "<p class='muted'>None</p>"

        course_sections += f"""
        <div class="course-card" style="border-left:4px solid {border_color}">
          <div class="course-header">
            <span class="course-title">{course['name']}</span>
            {status_label}
          </div>
          <div class="course-meta">
            Total files: <strong>{course['total_assets']}</strong>
            &nbsp;&nbsp;|&nbsp;&nbsp;
            Modules in manifest: <strong>{course['deck_count']}</strong>
            {"&nbsp;&nbsp;|&nbsp;&nbsp; Scanned from: <code style='background:#f5f5f5;padding:1px 6px;border-radius:3px'>" + course['true_root'] + "</code>" if course.get('true_root') and course['true_root'] != '.' else ""}
          </div>

          <div class="section-grid">
            <div>
              <h4>Deck / Module Status</h4>
              {deck_table}
            </div>
            <div>
              <h4>Asset Breakdown</h4>
              {asset_table}
            </div>
          </div>

          <div class="section-grid">
            <div>
              <h4>Course Gaps</h4>
              {gaps_html}
            </div>
            <div>
              <h4>Missing Thumbnails <small>(PPT/PPTX without cover image)</small></h4>
              {thumbs_html}
            </div>
          </div>
        </div>
        """

    filter_note = f'<p class="filter-note">Filtered to: <strong>{report["course_filter"]}</strong></p>' if report.get("course_filter") else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Ingestion Report</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f8f9fa; color: #212529; padding: 24px;
    }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    h1 {{ font-size: 1.8em; margin-bottom: 4px; color: #1a1a2e; }}
    h2 {{ font-size: 1.2em; margin: 24px 0 12px; color: #333; border-bottom: 2px solid #e0e0e0; padding-bottom: 6px; }}
    h3 {{ font-size: 1.05em; margin: 16px 0 8px; color: #444; }}
    h4 {{ font-size: 0.9em; margin: 0 0 8px; color: #555; text-transform: uppercase; letter-spacing: 0.05em; }}
    .meta {{ color: #666; font-size: 0.88em; margin-bottom: 20px; }}
    .filter-note {{ background: #e3f2fd; border-left: 3px solid #1976d2; padding: 8px 12px; margin-bottom: 16px; border-radius: 4px; }}

    /* PDF button */
    .pdf-btn {{
      display: inline-flex; align-items: center; gap: 8px;
      background: #1565c0; color: #fff; border: none; border-radius: 6px;
      padding: 10px 20px; font-size: 0.95em; font-weight: 600;
      cursor: pointer; text-decoration: none; margin-bottom: 20px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.2);
    }}
    .pdf-btn:hover {{ background: #0d47a1; }}

    /* Summary cards */
    .summary-grid {{
      display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px;
    }}
    .stat-card {{
      background: #fff; border-radius: 8px; padding: 16px 20px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.1); text-align: center;
      border-top: 4px solid #e0e0e0;
    }}
    .stat-card .num {{ font-size: 2.2em; font-weight: 700; color: #1a1a2e; }}
    .stat-card .label {{ font-size: 0.82em; color: #666; margin-top: 4px; }}
    .stat-card.warn  {{ border-top-color: #e65100; }}
    .stat-card.warn .num {{ color: #e65100; }}
    .stat-card.ok    {{ border-top-color: #2e7d32; }}
    .stat-card.ok .num {{ color: #2e7d32; }}
    .stat-card.blue  {{ border-top-color: #1565c0; }}
    .stat-card.blue .num {{ color: #1565c0; }}
    .progress-wrap {{ margin-top: 10px; }}
    .progress-bar-bg {{ background:#e0e0e0; border-radius:6px; height:8px; overflow:hidden; }}
    .progress-bar-fill {{ height:100%; border-radius:6px; transition:width 0.3s; }}

    /* Tables */
    table {{ border-collapse: collapse; width: 100%; font-size: 0.88em; margin-bottom: 8px; }}
    th {{ background: #f5f5f5; padding: 7px 12px; text-align: left; font-weight: 600; border-bottom: 2px solid #ddd; }}
    td {{ padding: 6px 12px; border-bottom: 1px solid #eee; }}
    tr:last-child td {{ border-bottom: none; }}

    /* Course cards */
    .course-card {{
      background: #fff; border-radius: 8px; margin-bottom: 20px;
      padding: 20px 24px; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    }}
    .course-header {{
      display: flex; align-items: center; gap: 12px; margin-bottom: 6px;
    }}
    .course-title {{ font-size: 1.05em; font-weight: 700; color: #1a1a2e; }}
    .course-meta {{ font-size: 0.85em; color: #666; margin-bottom: 16px; }}
    .section-grid {{
      display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 16px;
    }}
    .gap-list {{ padding-left: 18px; font-size: 0.87em; color: #c62828; }}
    .gap-list li {{ margin-bottom: 4px; }}
    .muted {{ color: #999; font-size: 0.85em; font-style: italic; }}

    /* Action required box */
    .action-box {{
      margin-top: 12px; background: #fff8e1; border: 1px solid #f9a825;
      border-left: 4px solid #f57f17; border-radius: 6px; padding: 12px 16px;
      font-size: 0.85em; color: #444;
    }}
    .action-box strong {{ color: #e65100; display: block; margin-bottom: 6px; font-size: 0.95em; }}
    .action-box p {{ margin-bottom: 6px; line-height: 1.5; }}
    .action-box code {{ background: #f5f5f5; padding: 1px 5px; border-radius: 3px; font-size: 0.9em; }}
    .action-steps {{ padding-left: 18px; margin-top: 6px; }}
    .action-steps li {{ margin-bottom: 4px; line-height: 1.5; }}

    /* Status legend */
    .legend {{
      display: grid; grid-template-columns: 1fr 1fr; gap: 12px;
      background: #fff; border-radius: 8px; padding: 20px 24px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 8px;
    }}
    .legend-item {{
      display: flex; align-items: flex-start; gap: 12px; font-size: 0.88em; color: #444;
    }}
    .legend-dot {{
      flex-shrink: 0; width: 14px; height: 14px; border-radius: 50%; margin-top: 3px;
    }}
    .legend-dot.pass  {{ background: #2e7d32; }}
    .legend-dot.fail  {{ background: #c62828; }}
    .legend-dot.retry {{ background: #e65100; }}
    .legend-dot.warn  {{ background: #f57f17; }}

    /* Print / PDF styles */
    @media print {{
      body {{ background: #fff; padding: 0; }}
      .pdf-btn {{ display: none !important; }}
      .course-card {{ box-shadow: none; border: 1px solid #ddd; page-break-inside: avoid; }}
      .summary-grid {{ grid-template-columns: repeat(3, 1fr); }}
      .section-grid {{ grid-template-columns: 1fr 1fr; }}
      .legend {{ box-shadow: none; border: 1px solid #ddd; page-break-inside: avoid; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Consolidated Ingestion Report</h1>
    <p class="meta">Generated: {report['generated_at']} &nbsp;|&nbsp; Source: {report['root_dir']}</p>
    {filter_note}

    <button class="pdf-btn" onclick="window.print()">
      &#x2B07; Download as PDF
    </button>

    <div class="summary-grid">
      <div class="stat-card blue">
        <div class="num">{s['total_courses']}</div>
        <div class="label">Total Courses</div>
      </div>
      <div class="stat-card blue">
        <div class="num">{s['total_assets']}</div>
        <div class="label">Total Assets</div>
      </div>
      <div class="stat-card ok">
        <div class="num">{s['courses_successful']}</div>
        <div class="label">Courses Successful</div>
        <div class="progress-wrap">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{round(s['courses_successful']/s['total_courses']*100) if s['total_courses'] else 0}%;background:#2e7d32"></div>
          </div>
          <small style="color:#2e7d32">{round(s['courses_successful']/s['total_courses']*100) if s['total_courses'] else 0}% complete</small>
        </div>
      </div>
      <div class="stat-card {'warn' if s['courses_remaining'] else 'ok'}">
        <div class="num">{s['courses_remaining']}</div>
        <div class="label">Courses Remaining</div>
        <div class="progress-wrap">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{round(s['courses_remaining']/s['total_courses']*100) if s['total_courses'] else 0}%;background:#e65100"></div>
          </div>
          <small style="color:#e65100">{round(s['courses_remaining']/s['total_courses']*100) if s['total_courses'] else 0}% need attention</small>
        </div>
      </div>
      <div class="stat-card {'warn' if s['courses_with_issues'] else 'ok'}">
        <div class="num">{s['courses_with_issues']}</div>
        <div class="label">Courses with Issues</div>
      </div>
      <div class="stat-card {'warn' if s['missing_thumbnails_total'] else 'ok'}">
        <div class="num">{s['missing_thumbnails_total']}</div>
        <div class="label">Missing Thumbnails</div>
      </div>
    </div>

    <h2>Status Legend</h2>
    <div class="legend">
      <div class="legend-item">
        <span class="legend-dot pass"></span>
        <div>
          <strong>PASS</strong> — File is readable and meets the minimum expected size for its type.
          The asset was successfully ingested and is ready for use.
        </div>
      </div>
      <div class="legend-item">
        <span class="legend-dot fail"></span>
        <div>
          <strong>FAIL</strong> — File is empty (0 bytes) or could not be read at all.
          This asset is broken and must be re-uploaded or replaced before ingestion can succeed.
        </div>
      </div>
      <div class="legend-item">
        <span class="legend-dot retry"></span>
        <div>
          <strong>RETRY</strong> — File exists but is suspiciously small for its type
          (XML / PPT / document &lt; 512 B &nbsp;|&nbsp; image / video &lt; 100 B).
          It may be truncated or corrupt. Re-upload the original file and re-run ingestion.
        </div>
      </div>
      <div class="legend-item">
        <span class="legend-dot warn"></span>
        <div>
          <strong>WARN</strong> — A module or deck was found in the manifest but contains
          no items. The structure exists but has no content linked to it yet.
        </div>
      </div>
    </div>

    <h2>Asset-Level Summary (All Courses)</h2>
    <table>
      <thead>
        <tr><th>Type</th><th>Total</th><th>Pass</th><th>Fail</th><th>Retry</th><th>Pass Rate</th></tr>
      </thead>
      <tbody>{asset_rows}</tbody>
    </table>

    <h2>Course Details</h2>
    {course_sections}
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate consolidated ingestion report")
    parser.add_argument("--course",   metavar="NAME", help="Filter to a specific course (substring match)")
    parser.add_argument("--root",     metavar="DIR",  default=str(ROOT_DIR), help=f"Uploads root directory (default: {ROOT_DIR})")
    parser.add_argument("--output",   metavar="FILE", default=str(OUTPUT_JSON), help=f"JSON output path (default: {OUTPUT_JSON})")
    parser.add_argument("--no-html",  action="store_true", help="Skip HTML report generation")
    args = parser.parse_args()

    root_dir    = Path(args.root)
    output_json = Path(args.output)
    output_html = output_json.with_suffix(".html")

    report = generate_report(root_dir, course_filter=args.course)

    print_report(report)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\nJSON report saved to: {output_json}")

    if not args.no_html:
        html = generate_html(report)
        with open(output_html, "w", encoding="utf-8") as fh:
            fh.write(html)
        print(f"HTML report saved to: {output_html}")


if __name__ == "__main__":
    main()
