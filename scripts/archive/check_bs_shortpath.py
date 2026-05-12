import sys
sys.path.insert(0, 'src')
from pathlib import Path
import ctypes, zipfile, tempfile, shutil

def get_short_path_name(long_name):
    import ctypes.wintypes as w
    buf = ctypes.create_unicode_buffer(w.MAX_PATH)
    ctypes.windll.kernel32.GetShortPathNameW(str(long_name), buf, w.MAX_PATH)
    return Path(buf.value)

base = Path('storage/uploads/BS Computer Science')
zips = list(base.glob('*.zip'))
if not zips:
    print('No zips')
    sys.exit(0)

zp = zips[0]
print(f'Long path: {zp}')
short = get_short_path_name(zp)
print(f'Short path: {short}')

extract_dir = Path(tempfile.mkdtemp())
try:
    with zipfile.ZipFile(short, 'r') as zf:
        zf.extractall(extract_dir)
    print('Extracted OK')
    # detect format
    from utils.format_detector import FormatDetector
    fmt = FormatDetector.detect(extract_dir)
    print('Format:', fmt.value)
    top = list(extract_dir.iterdir())
    print('Top items:', [t.name for t in top[:10]])
finally:
    shutil.rmtree(extract_dir, ignore_errors=True)
