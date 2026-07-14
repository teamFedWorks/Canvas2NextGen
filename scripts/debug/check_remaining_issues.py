"""
Investigate the 3 remaining issues:
1. Faculty Info - broken bbcswebdav/xid-22187558_1 img
2. Writing Rubric - empty anchor text
3. Syllabus PDF - empty Resource item
"""
import sys, zipfile, xml.etree.ElementTree as ET, html as hm, re
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
Z = ROOT / "storage/uploads/old/WBU/upload-manifest.zip"

with zipfile.ZipFile(Z) as zf:
    names = set(zf.namelist())

    # ── Faculty Information (res00104) ────────────────────────────────────────
    raw = zf.read("res00104.dat").decode("utf-8", errors="replace")
    root = ET.fromstring(raw)
    body_el = root.find(".//BODY/TEXT")
    body = hm.unescape(body_el.text) if (body_el is not None and body_el.text) else ""

    print("=== Faculty Info xid references ===")
    xids = re.findall(r"xid-(\d+_\d+)", body)
    for xid in set(xids):
        matches = [n for n in names if xid in n and not n.endswith(".xml")]
        status = matches[0] if matches else "NOT IN ZIP"
        print(f"  xid-{xid}: {status}")
    print()

    # ── Writing Rubric (res00105) ─────────────────────────────────────────────
    raw2 = zf.read("res00105.dat").decode("utf-8", errors="replace")
    root2 = ET.fromstring(raw2)
    body_el2 = root2.find(".//BODY/TEXT")
    body2 = hm.unescape(body_el2.text) if (body_el2 is not None and body_el2.text) else ""
    print("=== Writing Rubric raw body ===")
    print(body2[:600])
    print()
    # Extract data-bbfile JSON manually
    m = re.search(r'data-bbfile="(\{.*?\})"', body2)
    if not m:
        # Try with single quotes or no-quote wrapping
        m = re.search(r"data-bbfile='(\{.*?\})'", body2)
    if m:
        print("  data-bbfile JSON:", m.group(1)[:200])
    else:
        # Show the raw attribute region
        start = body2.find("data-bbfile")
        print("  data-bbfile region:", body2[start:start+300] if start != -1 else "NOT FOUND")
    print()

    # ── PDF Syllabus (res00090) ───────────────────────────────────────────────
    raw3 = zf.read("res00090.dat").decode("utf-8", errors="replace")
    root3 = ET.fromstring(raw3)
    files_el = root3.find(".//FILES")
    print("=== Syllabus FILES element ===")
    if files_el is not None:
        print(ET.tostring(files_el, encoding="unicode"))
    # Verify csfiles path
    xid_43978347 = [n for n in names if "43978347" in n and not n.endswith(".xml")]
    print("csfiles for xid-43978347:", xid_43978347)
    print()

    # Check what _walk_toc actually produces for res00090
    # by verifying the bb_type after our fix
    manifest_xml = zf.read("imsmanifest.xml").decode("utf-8", errors="replace")
    mroot = ET.fromstring(manifest_xml)
    BB_NS = "{http://www.blackboard.com/content-packaging/}"
    r90 = None
    for res in mroot.iter("resource"):
        if res.get("identifier", "") == "res00090":
            r90 = res
            break
    if r90 is not None:
        print("=== res00090 manifest entry ===")
        print(f"  type    : {r90.get('type','')}")
        print(f"  bb:file : {r90.get(f'{BB_NS}file','')}")
        print(f"  bb:title: {r90.get(f'{BB_NS}title','')}")
