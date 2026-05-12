import sys
sys.path.insert(0, 'src')
from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil

# Check just the first ZIP in BS Computer Science
base = Path('storage/uploads/BS Computer Science')
zips = list(base.glob('*.zip'))
print(f'Found {len(zips)} ZIPs in root')
if zips:
    zp = zips[0]
    print(f'Testing: {zp.name}')
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zp, 'r') as zf:
            zf.extractall(extract_dir)
        fmt = FormatDetector.detect(extract_dir)
        print(f'Format: {fmt.value}')
        # Count top-level items
        top = list(extract_dir.iterdir())
        print(f'Top-level items: {len(top)}')
        for it in top[:10]:
            print(' ', it.name, 'DIR' if it.is_dir() else 'FILE')
        # Check for manifest specifics
        manifest = extract_dir / 'imsmanifest.xml'
        if manifest.exists():
            content = manifest.read_text(encoding='utf-8', errors='replace')[:500]
            if 'blackboard.com/content-packaging' in content:
                print('Manifest indicates BLACKBOARD')
            elif 'imsglobal.org' in content:
                print('Manifest indicates Canvas IMS-CC')
            else:
                print('Manifest present but unknown namespace')
        else:
            print('No imsmanifest.xml')
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
else:
    print('No ZIP files found at root')
