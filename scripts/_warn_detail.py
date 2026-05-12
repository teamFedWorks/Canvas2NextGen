import json
from pathlib import Path
import sys

slugs = sys.argv[1:] if len(sys.argv) > 1 else []
outputs = Path("storage/outputs")

if not slugs:
    slugs = [f.stem.replace("validation_", "") for f in sorted(outputs.glob("validation_*.json"))]

for slug in slugs:
    f = outputs / f"validation_{slug}.json"
    if not f.exists():
        continue
    data = json.loads(f.read_text())
    auto = data.get("auto_import_rate", 0)
    tasks = len(data["manual_tasks"])
    print(f"\n{'='*70}")
    print(f"  {slug}  |  auto={auto}%  |  tasks={tasks}")
    print(f"{'='*70}")
    for mod in data["module_results"]:
        for item in mod["items"]:
            if item["status"] == "WARN":
                detail = item["detail"]
                if "Respondus" in detail:
                    tag = "[RESPONDUS]"
                elif "No content" in detail:
                    tag = "[EMPTY]    "
                else:
                    tag = "[OTHER]    "
                print(f"  {tag} [{mod['title'][:35]}] {item['title'][:55]}")
