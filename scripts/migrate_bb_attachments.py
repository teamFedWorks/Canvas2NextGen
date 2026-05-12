"""
One-time migration for existing MongoDB course records.

Blackboard Ultra attachments may appear as empty anchors with a
`data-bbfile` JSON attribute and `href="#"`. The frontend doesn't render
these, resulting in blank white boxes.

This script rewrites those anchors into a visible placeholder link.

Note: To get actual working downloads (real S3 URLs), you must re-import
the affected Blackboard courses after applying the pipeline fix.
"""

import json
import os
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup
from pymongo import MongoClient


def _resolve_file_name(bb_file_meta: Dict[str, Any], fallback: str) -> str:
    return (
        bb_file_meta.get("displayName")
        or bb_file_meta.get("linkName")
        or fallback
        or "Attachment"
    )


def _resolve_mime_type(bb_file_meta: Dict[str, Any]) -> str:
    return str(bb_file_meta.get("mimeType") or "")


def migrate() -> None:
    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DATABASE", "lms_db")

    if not mongo_uri:
        raise ValueError("Missing MONGODB_URI env var")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    courses = db["courses"]

    fixed_count = 0
    scanned = 0

    cursor = courses.find({"curriculum": {"$exists": True}})
    for course in cursor:
        scanned += 1
        modified = False

        curriculum = course.get("curriculum") or []
        for module in curriculum:
            items = module.get("items") or []
            for item in items:
                content = item.get("content") or ""
                if "data-bbfile" not in content:
                    continue

                soup = BeautifulSoup(content, "html.parser")
                anchors = soup.find_all("a", attrs={"data-bbfile": True})
                if not anchors:
                    continue

                for anchor in anchors:
                    raw = anchor.get("data-bbfile") or ""
                    try:
                        meta = json.loads(raw)
                    except Exception:
                        continue

                    file_name = _resolve_file_name(meta, anchor.get_text(strip=True))
                    mime_type = _resolve_mime_type(meta)

                    wrapper = soup.new_tag(
                        "div",
                        **{
                            "class": "attachment-wrapper",
                            "data-filename": file_name,
                            "data-mimetype": mime_type,
                        },
                    )
                    link = soup.new_tag(
                        "a",
                        href="#",
                        target="_blank",
                        **{"class": "bb-attachment-link missing-asset", "rel": "noopener noreferrer"},
                    )

                    if "pdf" in mime_type.lower():
                        link.string = f"View PDF: {file_name}"
                    elif "word" in mime_type.lower() or file_name.lower().endswith((".doc", ".docx")):
                        link.string = f"Download Word Doc: {file_name}"
                    else:
                        link.string = f"Download: {file_name}"

                    note = soup.new_tag("span", **{"class": "attachment-note"})
                    note.string = "(Attachment placeholder; re-import required to activate download.)"

                    wrapper.append(link)
                    wrapper.append(note)
                    anchor.replace_with(wrapper)
                    modified = True

                if modified:
                    item["content"] = str(soup)

        if modified:
            courses.update_one({"_id": course["_id"]}, {"$set": {"curriculum": curriculum}})
            fixed_count += 1
            print(f"Fixed: {course.get('title') or course['_id']}")

    print(f"Scanned {scanned} courses. Updated {fixed_count} courses.")


if __name__ == "__main__":
    migrate()

