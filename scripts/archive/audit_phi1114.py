import sys
sys.path.insert(0, 'src')
import xml.etree.ElementTree as ET
from pathlib import Path

manifest = Path(r'storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation/imsmanifest.xml')
tree = ET.parse(str(manifest))
root = tree.getroot()

ns = {
    'cc': 'http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1',
    'lom': 'http://ltsc.ieee.org/xsd/imsccv1p1/LOM/resource',
    'lomimscc': 'http://ltsc.ieee.org/xsd/imsccv1p1/LOM/manifest'
}

orgs = root.find('cc:organizations', ns)
items_count = 0
if orgs is not None:
    for org in orgs.findall('cc:organization', ns):
        for item in org.iter('{http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1}item'):
            items_count += 1

resources = root.find('cc:resources', ns)
res_count = len(resources.findall('cc:resource', ns)) if resources is not None else 0

print(f'Manifest items (leaf nodes): {items_count}')
print(f'Resources declared: {res_count}')

# Count files on disk
base = manifest.parent
files = list(base.rglob('*'))
print(f'Total files on disk: {len(files)}')

# Count specific types
xml_files = [f for f in files if f.suffix.lower() == '.xml']
html_files = [f for f in files if f.suffix.lower() == '.html']
qti_files = [f for f in files if 'assessment_qti' in f.name]
print(f'XML files: {len(xml_files)}')
print(f'HTML files: {len(html_files)}')
print(f'QTI files: {len(qti_files)}')

# Check course_settings
cs = base / 'course_settings'
if cs.exists():
    print(f'course_settings/ exists with {len(list(cs.iterdir()))} files')
