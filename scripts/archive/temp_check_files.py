import zipfile, tempfile, shutil, pathlib, re
zip_path = pathlib.Path('storage/uploads/WBU/phd-course-shell.zip')
extract_dir = pathlib.Path(tempfile.mkdtemp())
with zipfile.ZipFile(zip_path) as zf:
    zf.extractall(extract_dir)
items = list(extract_dir.iterdir())
if len(items)==1 and items[0].is_dir():
    extract_dir = items[0]

xids_needed = ['41796952_1','41795389_1','41796916_1','41796920_1','41796924_1','41796928_1','41796939_1','41796943_1']
cs_root = extract_dir / 'csfiles'
for xid in xids_needed:
    matches = list(cs_root.rglob(f'__xid-{xid}*'))
    print(f'{xid}: found {len(matches)} files -> {[m.name for m in matches]}')
shutil.rmtree(extract_dir)
