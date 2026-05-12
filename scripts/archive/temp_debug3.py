from adapters.blackboard_adapter import BlackboardAdapter, _BBContentReader
from pathlib import Path
import zipfile, tempfile, shutil, xml.etree.ElementTree as ET

zip_path = Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = Path(tempfile.mkdtemp(prefix='bb_rawread_'))
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items) == 1 and items[0].is_dir():
    extract_dir = items[0]

target_pages = ['Syllabus 5306', 'Annotated Bibliography Example', 'Annotated Bibliography Template']
manifest = Path(extract_dir) / 'imsmanifest.xml'
root = ET.parse(str(manifest)).getroot()
resources = {}
for res in root.find('resources').findall('resource'):
    ident = res.get('identifier','')
    bb_file = res.get('{http://www.blackboard.com/content-packaging/}file','')
    rtype = res.get('type','')
    title = res.get('{http://www.blackboard.com/content-packaging/}title','')
    resources[ident] = {'bb_file': bb_file, 'type': rtype, 'title': title}

orgs = root.find('organizations')
for org in orgs.findall('organization'):
    for item in org.findall('item'):
        def walk(item, depth=0):
            ident = item.get('identifierref','')
            title_el = item.find('title')
            title = title_el.text.strip() if title_el is not None and title_el.text else ''
            if title in target_pages:
                res_info = resources.get(ident,{})
                bb_file = res_info.get('bb_file')
                print(f'Found target page: {title} -> resource: {ident} -> file: {bb_file}')
                if bb_file:
                    dat = Path(extract_dir) / bb_file
                    if dat.exists():
                        content_raw = dat.read_text(encoding='utf-8', errors='replace')
                        count = content_raw.count('@X@EmbeddedFile')
                        print(f'  .dat file has {count} EmbeddedFile markers')
                        reader = _BBContentReader(dat)
                        print(f'  Reader body length: {len(reader.body_html)}')
                        print(f'  Body contains marker? {"@X@EmbeddedFile" in reader.body_html}')
                        idx = reader.body_html.find('@X@')
                        if idx >= 0:
                            print(f'  Snippet: {reader.body_html[idx:idx+100]}')
            for child in item.findall('item'):
                walk(child)
        walk(item)

shutil.rmtree(extract_dir)
