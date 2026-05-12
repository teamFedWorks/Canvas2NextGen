import sys
sys.path.insert(0, 'src')

from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil

zp = Path('storage/uploads/BS Computer Science/01 - PHI-1114 Logic and Argumentation.zip')
print('ZIP exists:', zp.exists())
if not zp.exists():
    print('NOT FOUND')
else:
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zp, 'r') as zf:
            zf.extractall(extract_dir)
        fmt = FormatDetector.detect(extract_dir)
        print('FORMAT:', fmt.value)
        top = list(extract_dir.iterdir())
        print('TOP LEVEL:')
        for it in top[:20]:
            print(' ', it.name, 'DIR' if it.is_dir() else 'FILE')
        if len(top) > 20:
            print('  ...', len(top)-20, 'more')
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
