#!/usr/bin/env python3
"""
Generate Final Course Onboarding Report
Reads storage/outputs/timing_report.json (written by run_timed_ingestion.py)
and builds storage/outputs/final_timing_report.md containing:
  1. The exact Course Onboarding & Validation effort table requested by the user.
  2. The exact pipeline execution runtimes.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
outputs_dir = ROOT / "storage" / "outputs"

def get_folder_stats(folder_path: Path):
    if not folder_path.exists():
        return 0, 0.0
    total_size = 0
    file_count = 0
    for p in folder_path.rglob('*'):
        if p.is_file():
            file_count += 1
            total_size += p.stat().st_size
    return file_count, total_size / (1024 * 1024) # MB

def classify_course(name: str, program: str, file_count: int, size_mb: float):
    # Determine size category
    if size_mb < 20 and file_count < 100:
        content_size = "Low"
    elif size_mb > 150 or file_count > 300:
        content_size = "High"
    else:
        content_size = "Medium"
        
    # Determine activities
    if program == "WBU":
        activities = "Blackboard conversion, file migration, link checking"
    elif content_size == "Low":
        activities = "Ingestion, basic asset upload"
    elif content_size == "High":
        if "Programming" in name or "Lab" in name or "Web" in name or "Multimedia" in name:
            activities = "Lab setup, file mapping, assessment QA"
        else:
            activities = "Ingestion, S3 asset migration, Respondus quiz handling, PPTX conversion, link mapping"
    else:
        # Medium
        if "Programming" in name or "Web" in name or "Scripting" in name:
            activities = "Ingestion, file mapping, assessment QA"
        else:
            activities = "Ingestion, module mapping, quiz validation"
            
    # Determine effort
    if content_size == "Low":
        onboarding = "2h"
        validation = "2h"
        total = "0.5 days"
        onboarding_h = 2.0
        validation_h = 2.0
        total_days = 0.5
    elif content_size == "High":
        onboarding = "6h"
        validation = "5h"
        total = "1.5 days"
        onboarding_h = 6.0
        validation_h = 5.0
        total_days = 1.5
    else:
        # Medium
        onboarding = "4h"
        validation = "4h"
        total = "1 day"
        onboarding_h = 4.0
        validation_h = 4.0
        total_days = 1.0
        
    return content_size, activities, onboarding, validation, total, onboarding_h, validation_h, total_days

def main():
    json_path = outputs_dir / "timing_report.json"
    if not json_path.exists():
        print(f"Error: {json_path} does not exist yet. Please run timed ingestion first.")
        sys.exit(1)
        
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    records = data.get("courses", [])
    
    lines = []
    lines.append("# SFC & WBU Course Onboarding & Validation Report\n")
    lines.append(f"**Report Generated:** {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}\n")
    lines.append("> [!NOTE]")
    lines.append("> This report contains two parts: the **Project Effort & Estimation Table** (based on course sizes and activities mapped to your effort model) ")
    lines.append("> and the **Exact Pipeline Execution Times** (representing the actual wall-clock seconds for machine-onboarding).\n")
    
    lines.append("## Part 1: Project Effort & Estimation Table\n")
    lines.append("| Course | Content Size | Activities | Onboarding | Validation | Total |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_onboarding_h = 0.0
    total_validation_h = 0.0
    total_days_sum = 0.0
    
    for r in records:
        folder_path = Path(r["folder_path"])
        file_count, size_mb = get_folder_stats(folder_path)
        
        c_size, c_act, c_on, c_val, c_tot, oh, vh, td = classify_course(
            r["course"], r["program"], file_count, size_mb
        )
        
        lines.append(f"| {r['course']} | {c_size} | {c_act} | {c_on} | {c_val} | {c_tot} |")
        
        total_onboarding_h += oh
        total_validation_h += vh
        total_days_sum += td
        
    # Add grand total row
    lines.append(f"| **GRAND TOTAL** | — | — | **{total_onboarding_h:.1f}h** | **{total_validation_h:.1f}h** | **{total_days_sum:.1f} days** |")
    lines.append("\n")
    
    lines.append("### Effort Summary")
    lines.append(f"- **Total Onboarding Human Effort:** {total_onboarding_h:.1f} hours")
    lines.append(f"- **Total Validation Human Effort:** {total_validation_h:.1f} hours")
    lines.append(f"- **Total Project Duration:** **{total_days_sum:.1f} days** (assuming an 8-hour workday)\n")
    
    lines.append("---\n")
    
    lines.append("## Part 2: Exact Ingestion Pipeline Run Times (Machine Ingestion)\n")
    lines.append("| # | Course | Program | Outcome | Ingestion Duration | Validation Report Path |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
    
    for idx, r in enumerate(records, 1):
        status_icon = {"success": "SUCCESS", "skipped": "SKIPPED", "failed": "FAILED", "crashed": "CRASHED"}.get(r["outcome"], "UNKNOWN")
        dur = r["elapsed_human"] if r["elapsed_human"] else "—"
        
        # Report paths reference
        report_dir = "SFC" if r["program"] != "WBU" else "WBU"
        clean_course_name = r["course"].lower().replace(" ", "-").replace("_", "-").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        # Try to find validation report file name format
        slug = r.get("course_id") or clean_course_name
        report_file_json = f"validation_{slug}.json"
        
        # Let's see if we can find the actual validation report file on disk
        import re
        course_lower = r["course"].lower()
        match = re.search(r'([a-z]+[-_]?\d+)', course_lower)
        code = match.group(1).replace("_", "-") if match else None
        
        if "phd-course-shell" in course_lower:
            code = "leadership-management-development"
            
        report_html_path = "—"
        sfc_dir = outputs_dir / "SFC"
        if sfc_dir.exists():
            for p in sfc_dir.glob("validation_*.html"):
                filename_lower = p.name.lower()
                if code and code in filename_lower:
                    report_html_path = f"[`{p.name}`](file:///B:/EduvateHub/CourseOnboarding/storage/outputs/SFC/{p.name})"
                    break
                
        lines.append(f"| {idx} | {r['course']} | {r['program']} | {status_icon} | **{dur}** | {report_html_path} |")
        
    lines.append(f"\n**Total Wall-Clock Time:** {data.get('total_elapsed_human', '—')}  ")
    lines.append(f"**Average Ingestion per Course:** {data.get('average_per_course', '—')}  ")
    lines.append(f"**Total Courses Successfully Processed:** {data.get('ingested', 0)} / {data.get('total_courses', 0)}\n")
    
    # Save the file
    md_path = outputs_dir / "final_timing_report.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Success: Report generated at {md_path}")

if __name__ == "__main__":
    main()
