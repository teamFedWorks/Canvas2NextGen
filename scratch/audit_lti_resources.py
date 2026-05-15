import lxml.etree as ET
from pathlib import Path

manifest_path = Path('storage/uploads/BS Information Technology/IT-2105 Programming II/imsmanifest.xml')
tree = ET.parse(str(manifest_path))
root = tree.getroot()
ns = {'ims': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1'}

res = root.findall('.//ims:resource', ns)
for r in res:
    t = r.get("type", "")
    if 'lti' in t.lower():
        print(f"LTI RESOURCE FOUND: {r.get('identifier')} -> {t}")
