import sys
sys.path.insert(0, 'src')

from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil

# Check all ZIPs under BS Computer Science
base = Path('storage/uploads/BS Computer Science')
zips = list(base.rglob('*.zip'))
print(f'Found {len(zips)} ZIP files')

for zp in zips[:5]:  # Show first 5
    print(f'\n--- {zp.relative_to(base)} ---')
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zp, 'r') as zf:
            zf.extractall(extract_dir)
        fmt = FormatDetector.detect(extract_dir)
        print(f'  Format: {fmt.value}')
        # Count top-level files
        items = list(extract_dir.iterdir())
        print(f'  Top-level: {len(items)} items')
        # Check for specific markers
        if (extract_dir / 'imsmanifest.xml').exists():
            print('  Has imsmanifest.xml')
        if (extract_dir / 'course_export.json').exists():
            print('  Has course_export.json (Canvas native export)')
        if (extract_dir / 'course_settings').exists():
            print('  Has course_settings/')
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
