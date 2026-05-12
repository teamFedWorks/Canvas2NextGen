import sys
sys.path.insert(0, 'src')
from utils.format_detector import FormatDetector
from pathlib import Path
import zipfile, tempfile, shutil, json

base = Path('storage/uploads/BS Computer Science')
zips = list(base.glob('*.zip'))
result = {
    "count": len(zips),
    "zips": [z.name for z in zips[:5]],
}
out = []
out.append(json.dumps(result, indent=2))

if zips:
    zp = zips[0]
    extract_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zp, 'r') as zf:
            zf.extractall(extract_dir)
        fmt = FormatDetector.detect(extract_dir)
        top = [it.name for it in extract_dir.iterdir()]
        result2 = {
            "format": fmt.value,
            "top": top[:15],
            "has_manifest": (extract_dir / 'imsmanifest.xml').exists()
        }
        out.append(json.dumps(result2, indent=2))
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

# Write to a file we can read
Path('bs_check_result.txt').write_text('\n'.join(out), encoding='utf-8')
print('WOKE')
