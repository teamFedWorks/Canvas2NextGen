import json, sys

slug = sys.argv[1] if len(sys.argv) > 1 else "phi-1114-01-26sp"
with open(f"storage/outputs/validation_{slug}.json") as f:
    r = json.load(f)

print(f"Verdict      : {r['verdict']}")
print(f"Manual tasks : {len(r['manual_tasks'])}")
print(f"Auto-import  : {r.get('auto_import_rate','?')}%")
print()

warn_items = []
for mod in r["module_results"]:
    for item in mod["items"]:
        if item["status"] == "WARN":
            warn_items.append((mod["title"][:45], item["title"][:60], item["detail"]))

print(f"=== WARN ITEMS ({len(warn_items)}) ===")
for mod_title, item_title, detail in warn_items:
    print(f"  [{mod_title}]")
    print(f"    {item_title}")
    print(f"    -> {detail}")
    print()
