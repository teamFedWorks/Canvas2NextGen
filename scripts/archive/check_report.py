"""
Print the validation report summary to see what the manual task is.
"""
import sys
sys.path.insert(0, 'src')

from pathlib import Path
import json

outputs = Path('storage/outputs/WBU')
reports = list(outputs.glob('validation_*.json'))
if not reports:
    print('No validation reports found')
    sys.exit(1)

latest = max(reports, key=lambda p: p.stat().st_mtime)
print(f'Latest report: {latest}')
data = json.loads(latest.read_text())

print(f"\nVerdict: {data.get('verdict_label', 'N/A')}")
print(f"Summary: {data.get('summary', 'N/A')}")
print(f"Manual tasks count: {len(data.get('manual_tasks', []))}")
print(f"Assets missing: {data.get('assets_missing', 0)}")
print(f"Missing files: {data.get('missing_files', 0)}")

if data.get('manual_tasks'):
    print('\n=== MANUAL TASKS ===')
    for i, task in enumerate(data['manual_tasks'], 1):
        print(f'\n--- Task {i} ---')
        if isinstance(task, dict):
            print(f'Type: {task.get("type", "N/A")}')
            print(f'Title: {task.get("title", "N/A")}')
            print(f'Description: {str(task.get("description", ""))[:300]}')
            if 'action' in task:
                print(f'Action: {task["action"][:200]}')
            if 'why' in task:
                print(f'Why: {str(task["why"])[:200]}')
        else:
            print(f'Raw: {str(task)[:300]}')
