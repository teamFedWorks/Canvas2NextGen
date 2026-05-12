import sys
sys.path.insert(0, 'src')
from pathlib import Path
import json

report_path = Path('storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.json')
data = json.loads(report_path.read_text())

print('Keys in report:', list(data.keys()))
print('\n--- Full manual_tasks ---')
for task in data.get('manual_tasks', []):
    print(json.dumps(task, indent=2)[:500])

print('\n--- Validation items by status ---')
statuses = {}
for item in data.get('items', []):
    st = item.get('status', 'UNK')
    statuses[st] = statuses.get(st, 0) + 1
print(statuses)

print('\n--- Any items with WARN? ---')
for item in data.get('items', []):
    if item.get('status') == 'WARN':
        print(f"- {item.get('title','?')} [{item.get('type','?')}] {item.get('detail','')[:100]}")
if not any(i.get('status') == 'WARN' for i in data.get('items', [])):
    print('None')

print('\n--- Any items with FAIL? ---')
for item in data.get('items', []):
    if item.get('status') == 'FAIL':
        print(f"- {item.get('title','?')} [{item.get('type','?')}] {item.get('detail','')[:100]}")
if not any(i.get('status') == 'FAIL' for i in data.get('items', [])):
    print('None')
