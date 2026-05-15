import sys
import json
from pathlib import Path
sys.path.insert(0, 'src')
from core.canonical_pipeline import CanonicalPipeline

courses = [
    {
        "id": "IT-1104",
        "path": "storage/uploads/BS Information Technology/IT-1104 Programming I"
    },
    {
        "id": "IT-2105",
        "path": "storage/uploads/BS Information Technology/IT-2105 Programming II"
    },
    {
        "id": "IT-2510",
        "path": "storage/uploads/BS Information Technology/IT-2510 Database Management Systems"
    },
    {
        "id": "IT-3301",
        "path": "storage/uploads/BS Information Technology/IT-3301 Project Management"
    }
]

results = {}

for course in courses:
    print(f"Processing {course['id']}...")
    p = CanonicalPipeline(course['path'])
    source = p._prepare_source()
    manifest = p._classify_source()
    canonical = p._parse_canonical(source, manifest)
    enriched = p._enrich(canonical)
    
    counts = {}
    total_items = 0
    for module in enriched.modules:
        for item in module.items:
            t = item.content_type.value
            counts[t] = counts.get(t, 0) + 1
            total_items += 1
            
    results[course['id']] = {
        "counts": counts,
        "modules": len(enriched.modules),
        "assessments": len(enriched.assessments),
        "assets": len(enriched.assets),
        "total_items": total_items
    }

print("\nFINAL COUNTS:")
print(json.dumps(results, indent=2))
