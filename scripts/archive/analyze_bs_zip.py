import sys
sys.path.insert(0, 'src')

from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil, json

zp = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation.zip')
if not zp.exists():
    print('ZIP NOT FOUND')
    sys.exit(1)

extract_dir = Path(tempfile.mkdtemp())
try:
    with zipfile.ZipFile(zp, 'r') as zf:
        zf.extractall(extract_dir)
    fmt = FormatDetector.detect(extract_dir)
    print(f'FORMAT:{fmt.value}')

    # Count structure
    if (extract_dir / 'imsmanifest.xml').exists():
        import xml.etree.ElementTree as ET
        manifest = ET.parse(str(extract_dir / 'imsmanifest.xml')).getroot()
        orgs = manifest.find('organizations')
        if orgs is not None:
            items_count = 0
            for org in orgs.findall('organization'):
                for item in org.iter('item'):
                    items_count += 1
            print(f'MANIFEST_ITEMS:{items_count}')

    resources = list(extract_dir.rglob('*'))
    print(f'TOTAL_FILES:{len(resources)}')
finally:
    shutil.rmtree(extract_dir, ignore_errors=True)
