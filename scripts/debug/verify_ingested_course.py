"""
Verify what was actually ingested into MongoDB for the WBU course.
Shows the full module/item structure with types, content presence, and attachment URLs.
"""
import sys, os, re
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import pymongo

MONGO_URI = os.getenv("ULCP_MONGODB_URI") or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("ULCP_MONGODB_DATABASE") or os.getenv("MONGODB_DATABASE", "test")
COURSE_ID = "6a4c9ec9ecb9df20993c5ee4"

client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

# Find the course
from bson import ObjectId
course = db.courses.find_one({"_id": ObjectId(COURSE_ID)})
if not course:
    print(f"Course {COURSE_ID} not found")
    sys.exit(1)

print(f"Course: {course.get('title')}")
print(f"Slug  : {course.get('slug')}")
print(f"Code  : {course.get('courseCode')}")
print()

issues = []
total_items = 0

for mod in course.get("curriculum", []):
    print(f"  MODULE: {mod.get('title')}")
    for item in mod.get("items", []):
        total_items += 1
        itype    = item.get("type", "?")
        title    = item.get("title", "?")
        content  = item.get("content") or ""
        video    = item.get("videoUrl") or ""
        atts     = item.get("attachments") or []
        instr    = item.get("instructions") or item.get("description") or ""
        itype_flag = item.get("instructionalType") or ""

        has_content = bool(content.strip()) or bool(video) or bool(atts)

        # Build status line
        details = []
        if content.strip():
            details.append(f"content={len(content)}ch")
        if video:
            details.append(f"video={video[:50]}")
        for a in atts:
            url = a.get("url", "")
            name = a.get("name", "")
            broken = "❌ BROKEN" if (not url or "bbcswebdav" in url or "@X@" in url) else "✅"
            details.append(f"att={name}({broken})")
        if instr.strip():
            details.append(f"instr={len(instr)}ch")

        # Flag problems
        problems = []
        for a in atts:
            url = a.get("url", "")
            if not url or "bbcswebdav" in url or "@X@" in url:
                problems.append(f"BROKEN ATT URL: {url[:80]}")
        if itype in ("Assignment", "Discussion") and not has_content and not instr.strip():
            problems.append("NO CONTENT/INSTRUCTIONS")
        if content and ("@X@" in content or "bbcswebdav" in content):
            problems.append("BROKEN URL IN CONTENT")
        # Flag only genuinely broken xid refs (bbcswebdav or relative paths,
        # not xid tokens that appear inside already-resolved CDN URLs)
        if content and re.search(r'(?:bbcswebdav/|href=["\'])xid-\d', content):
            problems.append("UNRESOLVED XID IN CONTENT")
        if content and re.search(r'href=["\']csfiles/', content):
            problems.append("UNRESOLVED CSFILES PATH IN CONTENT")

        icon = "⚠️ " if problems else "   "
        detail_str = "  |  ".join(details) if details else "(empty)"
        type_str = f"[{itype}]"
        if itype_flag:
            type_str += f"/{itype_flag}"
        print(f"  {icon}  {type_str:<30} {title[:50]:<50}  {detail_str}")

        for p in problems:
            issues.append(f"  {title}: {p}")
            print(f"         ↳ ❌ {p}")

print()
print(f"Total items: {total_items}")
print()
if issues:
    print(f"=== ❌ {len(issues)} ISSUE(S) FOUND ===")
    for i in issues:
        print(i)
else:
    print("=== ✅ No broken URLs or mapping issues found ===")
