import sys
sys.path.insert(0, 'src')
import xml.etree.ElementTree as ET
from pathlib import Path

# Read manifest using Kilo's read-like approach - but since we can't, use raw read
manifest_path = r'storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation/imsmanifest.xml'
# Actually, we can't reliably open due to path spacing restrictions.
# Instead, use the glob tool results we know from earlier and read just the manifest text via Path (maybe it's small enough to succeed? Let's try).
try:
    from pathlib import Path
    p = Path(manifest_path)
    content = p.read_text(encoding='utf-8', errors='replace')
    print('Read manifest OK')
except Exception as e:
    print(f'Cannot read manifest via Path: {e}')
    sys.exit(1)

root = ET.fromstring(content)
ns = {'cc': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1'}

orgs = root.find('cc:organizations', ns)
items_count = 0
if orgs is not None:
    for org in orgs.findall('cc:organization', ns):
        for item in org.iter('{http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1}item'):
            items_count += 1

resources = root.find('cc:resources', ns)
res_count = len(resources.findall('cc:resource', ns)) if resources is not None else 0

print(f'Manifest items: {items_count}')
print(f'Resources declared: {res_count}')

# Count files on disk using rglob - this will likely fail due to path spaces.
# Instead, rely on earlier glob listing: we saw 138 entries at top of that dir.
# Let's manually count known file types from earlier listing
base = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation')
try:
    all_files = list(base.rglob('*'))
    print(f'Total files on disk: {len(all_files)}')
    xml_cnt = sum(1 for f in all_files if f.suffix.lower() == '.xml')
    html_cnt = sum(1 for f in all_files if f.suffix.lower() == '.html')
    qti_cnt = sum(1 for f in all_files if 'assessment_qti' in f.name)
    print(f'XML: {xml_cnt}, HTML: {html_cnt}, QTI: {qti_cnt}')
except Exception as e:
    print(f'rglob failed: {e}')
