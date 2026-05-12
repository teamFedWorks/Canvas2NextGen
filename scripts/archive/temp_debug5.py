from pathlib import Path
import zipfile, tempfile, shutil

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_raw3_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

# Check the dat files that had markers
for dat_name in ['res00110.dat','res00111.dat','res00112.dat','res00113.dat',
                  'res00114.dat','res00115.dat','res00116.dat','res00117.dat']:
    dat = Path(extract_dir) / dat_name
    if dat.exists():
        content = dat.read_text(encoding='utf-8', errors='replace')
        # Find title and type from the XML
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(content)
            title = root.get('id','')
            # get TITLE child
            title_elem = root.find('TITLE')
            if title_elem is not None:
                title = title_elem.get('value','')
            # Also show type?
            type_elem = root.find('CONTENTTYPE')
            print(f'{dat_name}: title="{title[:80]}", hasEmbedded={content.count("@X@EmbeddedFile")}')
        except Exception as e:
            print(f'{dat_name}: parse error: {e}')

shutil.rmtree(extract_dir)
