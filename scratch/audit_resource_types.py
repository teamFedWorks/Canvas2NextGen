import lxml.etree as ET
from pathlib import Path

manifest_path = Path('storage/uploads/BS Information Technology/IT-2105 Programming II/imsmanifest.xml')
tree = ET.parse(str(manifest_path))
root = tree.getroot()
ns = {'ims': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1'}

res_map = {r.get('identifier'): r.get('type') for r in root.findall('.//ims:resource', ns)}
items = root.findall('.//ims:item', ns)

types_found = set()
for i in items:
    ref = i.get('identifierref')
    if ref:
        t = res_map.get(ref)
        if t:
            types_found.add(t)
            if 'lti' in t.lower():
                print(f"LTI FOUND: {ref} -> {t}")

print("\nAll types found in items:")
for t in sorted(types_found):
    print(f"  - {t}")
