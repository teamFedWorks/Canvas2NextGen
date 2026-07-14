"""
Inspect the exact content of items with broken xid/bbcswebdav URLs
and the empty PDF syllabus item.
"""
import sys, os
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import pymongo
from bson import ObjectId

MONGO_URI = os.getenv("ULCP_MONGODB_URI") or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("ULCP_MONGODB_DATABASE") or os.getenv("MONGODB_DATABASE", "test")
COURSE_ID = "6a4c9ec9ecb9df20993c5ee4"

client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

course = db.courses.find_one({"_id": ObjectId(COURSE_ID)})

targets = {
    "Faculty Information",
    "Management Information Systems Writing Rubric",
    "Syllabus MISM 5340 VC01 Fall 1 2026.pdf",
}

for mod in course.get("curriculum", []):
    for item in mod.get("items", []):
        title = item.get("title", "")
        if title in targets:
            print(f"=== {title} ===")
            print(f"  type     : {item.get('type')}")
            print(f"  instr.T  : {item.get('instructionalType')}")
            print(f"  content  : {(item.get('content') or '')[:600]}")
            print(f"  videoUrl : {item.get('videoUrl','')}")
            for a in item.get("attachments") or []:
                print(f"  attachment: name={a.get('name')} url={a.get('url','')[:100]}")
            print()
            targets.discard(title)
            if not targets:
                break
    if not targets:
        break
