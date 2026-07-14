"""
Diagnose the title-based redirect issues in the Blackboard adapter for upload-manifest.zip.
"""
import sys, zipfile, xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
Z = ROOT / "storage/uploads/old/WBU/upload-manifest.zip"
BB_NS = "{http://www.blackboard.com/content-packaging/}"

with zipfile.ZipFile(Z) as zf:
    names = set(zf.namelist())
    manifest_xml = zf.read("imsmanifest.xml").decode("utf-8", errors="replace")
    mroot = ET.fromstring(manifest_xml)

    all_resources = {}
    for res in mroot.iter("resource"):
        rid = res.get("identifier", "")
        all_resources[rid] = {
            "type": res.get("type", ""),
            "bb_file": res.get(f"{BB_NS}file", ""),
            "title": res.get(f"{BB_NS}title", ""),
        }

    # --- Issue 1: WBU eTextbook Access never redirects to richer type ---
    print("=== Issue 1: WBU eTextbook Access ===")
    for rid, r in all_resources.items():
        if r["title"].lower() == "wbu etextbook access":
            handler = ""
            bf = r["bb_file"]
            if bf and bf in names:
                root = ET.fromstring(zf.read(bf).decode("utf-8", errors="replace"))
                h = root.find(".//CONTENTHANDLER")
                handler = h.get("value", "") if h is not None else ""
                desc_el = root.find(".//DESCRIPTION")
                desc = desc_el.get("value", "")[:120] if desc_el is not None else ""
            print(f"  {rid}: manifest_type={r['type']}, handler={handler}, file={bf}")
            print(f"         desc: {desc}")
    print()

    # --- Issue 2: Week 7 & 8 wrongly redirected to announcement ---
    print("=== Issue 2: Week 7 & 8 redirect ===")
    for rid, r in all_resources.items():
        if "week 7" in r["title"].lower():
            print(f"  {rid}: type={r['type']}, title={repr(r['title'])}, file={r['bb_file']}")
    print("  -> res00054:")
    r54 = all_resources.get("res00054", {})
    print(f"     type={r54.get('type')}, title={repr(r54.get('title'))}, file={r54.get('bb_file')}")
    # Confirm what's in res00054
    bf54 = r54.get("bb_file", "")
    if bf54 and bf54 in names:
        root54 = ET.fromstring(zf.read(bf54).decode("utf-8", errors="replace"))
        title_el = root54.find(".//TITLE")
        t = title_el.get("value", "") if title_el is not None else ""
        handler_el = root54.find(".//CONTENTHANDLER")
        h = handler_el.get("value", "") if handler_el is not None else ""
        print(f"     dat title={repr(t)}, handler={h}")
    print()

    # --- Issue 3: PDF syllabus not being uploaded ---
    print("=== Issue 3: PDF Syllabus (res00090) ===")
    raw90 = zf.read("res00090.dat").decode("utf-8", errors="replace")
    root90 = ET.fromstring(raw90)
    handler90 = root90.find(".//CONTENTHANDLER")
    print(f"  handler: {handler90.get('value','') if handler90 is not None else ''}")
    files90 = root90.find(".//FILES")
    if files90 is not None:
        for f in files90.findall(".//FILE"):
            name_el = f.find("NAME")
            name = name_el.text if name_el is not None else ""
            linkname_el = f.find("LINKNAME")
            linkname = linkname_el.text if linkname_el is not None else ""
            storage_el = f.find("STORAGETYPE")
            storage = storage_el.get("value", "") if storage_el is not None else ""
            print(f"  FILE: NAME={name}, LINKNAME={linkname}, STORAGETYPE={storage}")
            # Try to find it in csfiles
            xid = name.lstrip("/").replace("xid-", "")
            xid_key = name.split("/")[-1].lstrip("_")
            matches = [n for n in names if xid_key in n or (xid and xid in n)]
            print(f"  csfile matches for {repr(name)}: {matches[:3]}")
    print()

    # --- Issue 4: Missing assignment descriptions (res00117 = Assignment 2 should be assignment) ---
    print("=== Issue 4: Assignment redirects (should be Assignment not Discussion) ===")
    # In the manifest, all Weekly assignments link to INDIRECT discussionboard entries.
    # But the QTI test files (res00041 = Assignment 2) have subtype=Assignment.
    # The problem: the title redirect for 'Assignment 2' picks res00061 (discussionboard) 
    # over res00041 (qti-test with subtype=Assignment) because discussionboard wins
    for name_check in ["Assignment 2", "Assignment 1", "Assignment 3", "Assignment 4"]:
        matching = [(rid, r) for rid, r in all_resources.items()
                    if r["title"] == name_check]
        for rid, r in matching:
            bf = r["bb_file"]
            extra = ""
            if bf and bf in names:
                try:
                    rroot = ET.fromstring(zf.read(bf).decode("utf-8", errors="replace"))
                    meta = rroot.find(".//assessmentmetadata")
                    if meta is not None:
                        st = meta.find("bbmd_assessment_subtype")
                        extra = f" [subtype={st.text if st is not None else 'none'}]"
                    else:
                        h = rroot.find(".//CONTENTHANDLER")
                        if h is not None:
                            extra = f" [handler={h.get('value','')}]"
                except Exception:
                    pass
            print(f"  {rid}: type={r['type']}, file={bf}{extra}")
    print()

    print("=== Summary of richer_types priority check ===")
    richer_types = ("assessment/x-bb-qti-test", "resource/x-bb-discussionboard",
                    "resource/x-bb-announcement", "resource/x-bb-weblink")
    for name_check in ["Assignment 2", "Final Project", "Required First Assignment"]:
        candidates = [(rid, r) for rid, r in all_resources.items() if r["title"] == name_check]
        print(f"  {name_check}:")
        for rid, r in candidates:
            is_richer = r["type"] in richer_types
            print(f"    {rid}: {r['type']} {'(RICHER)' if is_richer else ''}")
