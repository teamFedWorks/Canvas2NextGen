import sys
sys.path.insert(0, 'src')

from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil

zp = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation.zip')
print(f'Checking: {zp}')
if not zp.exists():
    print('ZIP not found!')
    sys.exit(1)

extract_dir = Path(tempfile.mkdtemp())
try:
    with zipfile.ZipFile(zp, 'r') as zf:
        zf.extractall(extract_dir)
    fmt = FormatDetector.detect(extract_dir)
    print(f'Format: {fmt.value}')
    items = list(extract_dir.iterdir())
    print(f'Top-level items: {len(items)}')
    for item in items[:15]:
        kind = 'DIR' if item.is_dir() else 'FILE'
        print(f'  [{kind}] {item.name}')
    if len(items) > 15:
        print(f'  ... and {len(items)-15} more')
finally:
    shutil.rmtree(extract_dir, ignore_errors=True)
