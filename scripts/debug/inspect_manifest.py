"""
Inspect the upload-manifest.zip to understand all resource types and
identify what content each resource carries (URL, description, handler, file refs).
"""
import sys, zipfile, xml.etree.ElementTree as ET, html as html_mod
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

Z = ROOT / "storage/uploads/old/WBU/upload-manifest.zip"

with zipfile.ZipFile(Z) as zf:
    names = set(zf.namelist())
    manifest_xml = zf.read("imsmanifest.xml").decode("utf-8", errors="replace")
    mroot = ET.fromstring(manifest_xml)

    # Build resource lookup from manifest
    resource_map = {}  # identifier -> {bb:file, href, type}
    for res in mroot.iter("resource"):
        rid = res.get("identifier", "")
        resource_map[rid] = {
            "bb_file": res.get("{http://www.blackboard.com/content-packaging/}file", ""),
            "href": res.get("href", ""),
            "type": res.get("type", ""),
        }

    # Collect all item refs
    def collect_items(el, depth=0, path=""):
        items = []
        for item in el.findall("item"):
            title_el = item.find("title")
            t = title_el.text.strip() if title_el is not None and title_el.text else ""
            ref = item.get("identifierref", "")
            items.append((depth, ref, t))
            items.extend(collect_items(item, depth + 1, path + "/" + t))
        return items

    org = mroot.find("./organizations/organization")
    all_items = collect_items(org)

    print(f"{'='*80}")
    print(f"ZIP: {Z.name}  |  {len(names)} files  |  {len(all_items)} manifest items")
    print(f"{'='*80}\n")

    for depth, res_id, title in all_items:
        indent = "  " * depth
        rinfo = resource_map.get(res_id, {})

        # Parse the .dat file
        dat_fname = rinfo.get("bb_file") or (f"{res_id}.dat" if f"{res_id}.dat" in names else None)
        handler = ""
        desc = ""
        url = ""
        body_text = ""
        xml_error = ""

        if dat_fname and dat_fname in names:
            raw = zf.read(dat_fname).decode("utf-8", errors="replace")
            try:
                xroot = ET.fromstring(raw)
                h = xroot.find(".//CONTENTHANDLER")
                handler = h.get("value", "") if h is not None else ""
                d = xroot.find(".//DESCRIPTION")
                desc = d.get("value", "") if d is not None else ""
                u = xroot.find(".//URL")
                url = u.get("value", "") if u is not None else ""
                b = xroot.find(".//BODY/TEXT")
                if b is not None and b.text:
                    body_text = html_mod.unescape(b.text)[:200]
            except Exception as e:
                xml_error = str(e)

        print(f"{indent}[{res_id}] {title}")
        print(f"{indent}  handler : {handler or rinfo.get('type','')}")
        if desc:
            print(f"{indent}  desc    : {desc[:150]}")
        if url:
            print(f"{indent}  url     : {url}")
        if body_text:
            print(f"{indent}  body    : {body_text[:120]}")
        if xml_error:
            print(f"{indent}  ERROR   : {xml_error}")
        print()
