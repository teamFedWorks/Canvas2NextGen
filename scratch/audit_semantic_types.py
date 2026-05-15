from pathlib import Path
import sys
import os

# Add src to path
sys.path.insert(0, str(Path(os.getcwd()) / "src"))

from core.canonical_pipeline import CanonicalPipeline

courses = [
    'storage/uploads/BS Information Technology/IT-3301 Project Management'
]

keywords = [
    'article', 'webinar', 'zoom', 'survey', 'feedback', 'evaluation', 
    'announcement', 'welcome', 'textbook', 'reading', 'chapter', 
    'tutorial', 'guide', 'lti', 'turnitin'
]

for c in courses:
    print(f"\n=== Analyzing: {c} ===")
    p = CanonicalPipeline(c)
    source = p._prepare_source()
    classification = p._classify_source()
    canonical = p._parse_canonical(source, classification)
    
    # Run enrichment
    canonical = p._enrich(canonical)
    
    counts = {}
    for m in canonical.modules:
        for i in m.items:
            t = i.content_type.value
            counts[t] = counts.get(t, 0) + 1
    
    print("\nSemantic Classification Summary:")
    for t, count in sorted(counts.items()):
        print(f"  {t}: {count}")
    
    print("\nDetail of classified items (non-Lesson):")
    for m in canonical.modules:
        for i in m.items:
            if i.content_type.value != "Lesson":
                print(f"  [{i.content_type.value}] {i.title}")


