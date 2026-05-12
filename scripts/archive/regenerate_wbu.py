import sys
sys.path.insert(0, 'src')

from scripts.validate_ingestion import run_validation, save_report
from pathlib import Path

# Get course ID from the latest report
latest_report = Path('storage/outputs/WBU')
reports = list(latest_report.glob('validation_*.json'))
if reports:
    latest = max(reports, key=lambda p: p.stat().st_mtime)
    import json
    data = json.loads(latest.read_text())
    course_id = data.get('course_id')
    slug = data.get('slug')
    print(f'Re-validating course: {data.get("course_title")}')
    print(f'Course ID: {course_id}, Slug: {slug}')
    
    # Re-run validation
    rep = run_validation(course_id, by_slug=False, strict=False, quiet=False)
    
    # Save report (overwrites old)
    out_dir = Path('storage/outputs/WBU')
    html_path = save_report(rep, out_dir, emit_json=True)
    
    print(f'\nNew report saved: {html_path}')
    print(f'Verdict: {rep.verdict_label}')
    print(f'Manual tasks: {len(rep.manual_tasks)}')
    print(f'Auto-import rate: {rep.auto_import_rate:.1f}%')
else:
    print('No existing report found - need course_id to run validation')
    sys.exit(1)
