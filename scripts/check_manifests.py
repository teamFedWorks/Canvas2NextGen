#!/usr/bin/env python3
"""Check manifest TOC for structural patterns across courses."""
import xml.etree.ElementTree as ET
from pathlib import Path

NS = "{http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1}"

COURSES = {
    "IT-2440 (Scripting)": "storage/uploads/BS_Computer_Science/04_-_IT-2440_Scripting_Languages",
    "IT-3301 (Project Mgmt)": "storage/uploads/BS_Computer_Science/08_-_IT-3301_Project_Management",
    "IT-2510 (Database)": "storage/uploads/BS Information Technology/IT-2510 Database Management Systems",
    "IT-2105 (Programming II)": "storage/uploads/BS Information Technology/IT-2105 Programming II",
    "ENT-1777 (Design Thinking)": "storage/uploads/BS Information Technology/ENT-1777 Design Thinking and Innovation",
    "IT-3101 (IT Law)": "storage/uploads/BS_Computer_Science/07_-_IT-3101_Information_Tech_Law_and_Ethics",
}

ROOT = Path(__file__).parent.parent

# Patterns to flag as potentially structural/misclassified
SUSPICIOUS_LOWER = [
    "module:", "advanced learning", "graded assign", "assignment - part",
    "task", "tasks", "no assignment", "week 1 task", "week 2 task",
    "assignments", "individual assignment", "module: lesson",
]

def get_title(elem):
    t = elem.find(f"{NS}title") or elem.find("title")
    return (t.text or "").strip() if t is not None else "(no title)"

def walk_items(elem, depth=0):
    """Recursively walk <item> elements."""
    for child in elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "item":
            title = get_title(child)
            tl = title.lower()
            flag = " ◀ SUSPICIOUS" if any(s in tl for s in SUSPICIOUS_LOWER) else ""
            indent = "  " * depth
            if depth == 0:
                print(f"\n{indent}[MODULE] {title}")
            else:
                print(f"{indent}  {title}{flag}")
            walk_items(child, depth + 1)

for code, path in COURSES.items():
    manifest = ROOT / path / "imsmanifest.xml"
    if not manifest.exists():
        print(f"\n{code}: manifest NOT FOUND")
        continue

    tree = ET.parse(str(manifest))
    mroot = tree.getroot()

    # Find organizations — try namespaced first
    orgs = mroot.find(f"{NS}organizations") or mroot.find("organizations")
    if orgs is None:
        print(f"\n{code}: organizations NOT FOUND")
        continue

    print(f"\n{'='*65}")
    print(f"COURSE: {code}")
    print(f"{'='*65}")

    for org in orgs:
        tag = org.tag.split("}")[-1] if "}" in org.tag else org.tag
        if tag == "organization":
            walk_items(org, 0)
