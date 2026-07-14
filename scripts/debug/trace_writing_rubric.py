"""
Trace exactly what happens to the Writing Rubric item through the pipeline
to diagnose why anchor text stays empty/wrong.
"""
import sys, os, re
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import zipfile, xml.etree.ElementTree as ET, html as hm
from adapters.blackboard_adapter import _clean_bb_html

Z = ROOT / "storage/uploads/old/WBU/upload-manifest.zip"

with zipfile.ZipFile(Z) as zf:
    # 1. Get the raw body of res00105 (Writing Rubric)
    raw = zf.read("res00105.dat").decode("utf-8", errors="replace")
    root_el = ET.fromstring(raw)
    body_el = root_el.find(".//BODY/TEXT")
    raw_body = body_el.text if (body_el is not None and body_el.text) else ""
    print("=== 1. Raw body from dat file ===")
    print(raw_body[:400])
    print()

    # 2. After _clean_bb_html
    cleaned = _clean_bb_html(raw_body)
    print("=== 2. After _clean_bb_html ===")
    print(cleaned[:400])
    print()

    # 3. What the _bbfile_xid_to_name pre-scan finds in the cleaned content
    import html as _html_mod, json as _json_mod
    _bbfile_xid_to_name = {}
    for raw_match in re.finditer(r'<a[^>]+data-bbfile[^>]*>', cleaned, re.IGNORECASE | re.DOTALL):
        raw_tag = raw_match.group(0)
        brace_start = raw_tag.find('{')
        brace_end = raw_tag.rfind('}')
        print(f"  Found data-bbfile anchor. brace_start={brace_start}, brace_end={brace_end}")
        if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
            candidate = raw_tag[brace_start:brace_end + 1]
            candidate = _html_mod.unescape(_html_mod.unescape(candidate)).replace('&quot;', '"')
            try:
                meta = _json_mod.loads(candidate)
                name = meta.get('linkName') or meta.get('displayName') or ''
                print(f"  Parsed JSON OK: linkName={name}")
                xid_m = re.search(r'xid-(\d+_\d+)', raw_tag, re.IGNORECASE)
                if xid_m:
                    _bbfile_xid_to_name[xid_m.group(1)] = name
                    print(f"  Keyed by xid: {xid_m.group(1)} -> {name}")
            except Exception as e:
                print(f"  JSON parse failed: {e}")
                print(f"  Candidate: {candidate[:200]}")
        else:
            print("  No braces found in anchor tag")
        print(f"  Raw tag: {raw_tag[:300]}")
    print()
    print(f"  _bbfile_xid_to_name: {_bbfile_xid_to_name}")
    print()

    # 4. What BS4 sees after parsing the cleaned content
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(cleaned, 'html.parser')
    for a in soup.find_all('a'):
        if 'data-bbfile' in str(a) or 'xid-43978128' in str(a) or 'bbcswebdav' in str(a):
            print(f"=== 4. BS4 anchor ===")
            print(f"  str(a): {str(a)[:300]}")
            print(f"  a.get('href'): {a.get('href','')}")
            print(f"  a.get('data-bbfile'): {a.get('data-bbfile','')[:100]}")
            print(f"  a.get_text(): {a.get_text()}")
            # xid in href?
            href = a.get('href','')
            xid_m = re.search(r'xid-(\d+_\d+)', href, re.IGNORECASE)
            print(f"  xid in href: {xid_m.group(1) if xid_m else 'NONE'}")
            # lookup
            xid = xid_m.group(1) if xid_m else ''
            print(f"  lookup result: {_bbfile_xid_to_name.get(xid, 'NOT FOUND')}")
