import sys
sys.path.insert(0, 'src')

from pathlib import Path
import json

# Find the latest WBU validation report
outputs = Path('storage/outputs/WBU')
reports = list(outputs.glob('validation_*.json'))
if not reports:
    print('No reports found')
    sys.exit(1)

latest = max(reports, key=lambda p: p.stat().st_mtime)
print(f'Latest report: {latest}')

data = json.loads(latest.read_text())

# Print summary without S3 URLs
print(f'\nVerdict: {data.get("verdict_label")}')
print(f'Course: {data.get("course_title")}')
print(f'Institution: {data.get("institution_name")}')
print(f'Slug: {data.get("slug")}')
print(f'\nStatistics:')
print(f'  Total modules: {data.get("total_modules")}')
print(f'  Total items: {data.get("total_items")}')
print(f'  Items passed: {data.get("items_pass")}')
print(f'  Items warned: {data.get("items_warn")}')
print(f'  Items skipped: {data.get("items_skip")}')
print(f'  Auto-import rate: {data.get("auto_import_rate")}%')
print(f'\nAssets:')
print(f'  Total: {data.get("total_assets")}')
print(f'  Passed: {data.get("assets_pass")}')
print(f'  Failed: {data.get("assets_fail")}')
print(f'  Retry: {data.get("assets_retry")}')
print(f'  Missing: {data.get("assets_missing", 0)}')

print(f'\nManual tasks required: {len(data.get("manual_tasks", []))}')
for i, task in enumerate(data.get('manual_tasks', []), 1):
    print(f'\n  Task {i}:')
    if isinstance(task, dict):
        print(f'    Type: {task.get("type", "N/A")}')
        print(f'    Title: {task.get("title", "N/A")}')
        desc = task.get('description', task.get('why', ''))
        print(f'    Description: {str(desc)[:300]}')
        if 'action' in task:
            print(f'    Action: {str(task["action"])[:200]}')
    else:
        print(f'    Raw: {str(task)[:300]}')

# Print structure checks
print('\n\n=== STRUCTURE CHECKS ===')
for check in data.get('structure_checks', []):
    field = check.get('field', 'Unknown')
    status = check.get('status', 'UNK')
    value = check.get('value', '')
    action = check.get('action', '')
    if status in ('FAIL', 'WARN'):
        print(f'  [{status}] {field}: {value}')
        if action:
            print(f'         Action: {action}')
    else:
        print(f'  [PASS] {field}')
