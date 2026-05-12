import json
from pathlib import Path

outputs = Path("storage/outputs")
reports = sorted(outputs.glob("validation_*.json"))

print(f"{'Course':<35} {'Verdict':<8} {'Auto%':<7} {'Tasks':<6} {'Respondus':<10}")
print("=" * 80)

for r in reports:
    try:
        data = json.loads(r.read_text())
        slug = data['slug']
        verdict = data['verdict']
        auto_rate = data.get('auto_import_rate', 0)
        tasks = len(data['manual_tasks'])
        
        # Count Respondus items
        respondus_count = 0
        for mod in data['module_results']:
            for item in mod['items']:
                if 'Respondus' in item.get('detail', '') or 'Respondus' in item.get('why', ''):
                    respondus_count += 1
        
        print(f"{slug:<35} {verdict:<8} {auto_rate:>5.1f}% {tasks:>5} {respondus_count:>9}")
    except:
        pass
