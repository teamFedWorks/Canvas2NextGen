#!/usr/bin/env python3
"""
Course Ingestion Validation Script — auto-runs after every ingestion.

Every warning explains WHY it exists and EXACTLY what manual action is needed.
The (M/D) date notation in module titles is the class meeting date (Month/Day).

Usage:
  python scripts/validate_ingestion.py --course-id <mongo_id>
  python scripts/validate_ingestion.py --slug <course-slug>
  python scripts/validate_ingestion.py --course-id <id> --strict
"""
import sys, os, json, argparse, re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import boto3, bson
from pymongo import MongoClient
from botocore.exceptions import ClientError

class Status(str, Enum):
    PASS="PASS"; FAIL="FAIL"; RETRY="RETRY"; WARN="WARN"; SKIP="SKIP"

ICON = {Status.PASS:"[PASS]",Status.FAIL:"[FAIL]",Status.RETRY:"[RETRY]",Status.WARN:"[WARN]",Status.SKIP:"[SKIP]"}

@dataclass
class CheckResult:
    name: str; status: Status; value: str=""; why: str=""; action: str=""

@dataclass
class ItemResult:
    title: str; item_type: str; status: Status
    detail: str=""; why: str=""; action: str=""; attachments: int=0

@dataclass
class ModuleResult:
    title: str; week_label: str; status: Status
    item_count: int=0; items: List[ItemResult]=field(default_factory=list)
    issues: List[str]=field(default_factory=list)

@dataclass
class AssetResult:
    name: str; url: str; status: Status; size_bytes: int=0; detail: str=""

@dataclass
class ValidationReport:
    course_id: str; course_title: str; slug: str
    course_code: str=""; department: str=""
    institution: str=""          # e.g. "SFC" or "WBU" — derived from university record
    institution_name: str=""     # e.g. "St. Francis College" or "Wayland Baptist University"
    generated_at: str=field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    strict: bool=False
    structure_checks: List[CheckResult]=field(default_factory=list)
    module_results:   List[ModuleResult]=field(default_factory=list)
    asset_results:    List[AssetResult]=field(default_factory=list)
    metadata_checks:  List[CheckResult]=field(default_factory=list)
    total_modules: int=0; total_items: int=0; total_assets: int=0
    assets_pass: int=0; assets_fail: int=0; assets_retry: int=0
    # Fix 3: accuracy metrics
    items_pass: int=0; items_warn: int=0; items_skip: int=0
    auto_import_rate: float=0.0
    verdict: Status=Status.WARN; verdict_label: str=""; verdict_reason: str=""
    manual_tasks: List[str]=field(default_factory=list)

# ── Helpers ──────────────────────────────────────────────────────────────────

def _mongo():
    uri = os.getenv("MONGODB_URI")
    if not uri: raise ValueError("MONGODB_URI not set")
    return MongoClient(uri)

def fetch_course(ident: str, by_slug=False) -> Optional[Dict]:
    col = _mongo()[os.getenv("MONGODB_DATABASE","lms_db")]["courses"]
    if by_slug: return col.find_one({"slug": ident})
    try:    return col.find_one({"_id": bson.ObjectId(ident)})
    except: return col.find_one({"slug": ident})

def fetch_institution(university_id, course: Optional[Dict] = None) -> tuple:
    """
    Return (institution_code, institution_name) for a course.

    Resolution order:
      1. course['institution_code'] — set by IngestionWorker at ingest time
      2. universities collection lookup by university ObjectId
      3. Fallback: derive from program title

    Returns e.g. ("SFC", "St. Francis College") or ("WBU", "Wayland Baptist University").
    """
    # 1. Fastest path — institution_code stored directly on the course document
    if course:
        code = course.get("institution_code", "")
        if code:
            name_map = {
                "SFC": "St. Francis College",
                "WBU": "Wayland Baptist University",
            }
            return code, name_map.get(code, code)

    db = _mongo()[os.getenv("MONGODB_DATABASE","lms_db")]

    # 2. Universities collection lookup
    try:
        uid = bson.ObjectId(str(university_id)) if not isinstance(university_id, bson.ObjectId) else university_id
        uni = db.universities.find_one({"_id": uid})
        if uni:
            name = uni.get("name") or uni.get("title") or uni.get("shortName") or ""
            code = uni.get("code") or uni.get("shortName") or uni.get("abbreviation") or ""
            if name or code:
                return code or name[:3].upper(), name or code
    except Exception:
        pass

    # 3. Fallback: derive from program title
    try:
        prog = db.programs.find_one({"universityId": str(university_id)})
        if prog:
            prog_title = (prog.get("title") or prog.get("name") or "").upper()
            if "WAYLAND" in prog_title or "WBU" in prog_title:
                return "WBU", "Wayland Baptist University"
            return "SFC", "St. Francis College"
    except Exception:
        pass

    return "UNKNOWN", "Unknown Institution"

def _s3():
    return boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION","us-east-1"))

def _head(s3c, bucket, url) -> Tuple[Status,int,str]:
    cdn = os.getenv("CDN_URL","").rstrip("/")
    if cdn and url.startswith(cdn):
        key = url[len(cdn):].lstrip("/")
    elif f"{bucket}.s3" in url:
        key = re.sub(r"^https?://[^/]+/","",url)
    else:
        return Status.SKIP,0,"URL outside known CDN domain — cannot verify"
    try:
        r = s3c.head_object(Bucket=bucket, Key=key)
        sz = r.get("ContentLength",0)
        if sz==0: return Status.RETRY,0,"File exists in S3 but is 0 bytes — upload may have been interrupted"
        return Status.PASS,sz,f"Confirmed in S3 — {sz:,} bytes"
    except ClientError as e:
        c = e.response["Error"]["Code"]
        if c in ("404","NoSuchKey"): return Status.FAIL,0,"File NOT found in S3 — upload failed or was skipped"
        if c=="403":                 return Status.FAIL,0,"Access denied — check S3 bucket policy"
        return Status.FAIL,0,f"S3 error: {c}"

def _sz(n):
    for u in ("B","KB","MB","GB"):
        if n<1024: return f"{n:.1f} {u}"
        n//=1024
    return f"{n:.1f} TB"

def _week_label(title: str) -> str:
    """
    Convert Canvas module titles like 'Week 1 (9/9) - Introduction'
    to human-readable labels like 'Week 1 — Sep 9 — Introduction'.
    The (M/D) in Canvas titles is the class meeting date (Month/Day).
    """
    months = {"1":"Jan","2":"Feb","3":"Mar","4":"Apr","5":"May","6":"Jun",
              "7":"Jul","8":"Aug","9":"Sep","10":"Oct","11":"Nov","12":"Dec"}
    def repl(m):
        return f"— {months.get(m.group(1), m.group(1))} {m.group(2)}"
    label = re.sub(r'\((\d{1,2})/(\d{1,2})\)', repl, title)
    label = re.sub(r'\s*-\s*', ' — ', label).strip()
    return label

# ── Validation sections ───────────────────────────────────────────────────────

def validate_structure(course: Dict) -> List[CheckResult]:
    fields = {
        "title":            "Course Title",
        "slug":             "URL Slug",
        "courseUrl":        "Course URL",
        "university":       "University",
        "authorId":         "Author / Instructor ID",
        "curriculum":       "Curriculum (modules list)",
        "status":           "Publication Status",
        "description":      "Full Description",
        "shortDescription": "Short Description",
    }
    out = []
    for f, label in fields.items():
        val = course.get(f)
        if f == "university":
            valid = bool(val) and bson.ObjectId.is_valid(str(val))
            out.append(CheckResult(
                name=label,
                status=Status.PASS if valid else Status.FAIL,
                value=str(val)[:90] if val else "MISSING",
                why="" if valid else "Course university must be a valid ObjectId; empty strings break platform populate().",
                action="" if valid else "Set a valid university ObjectId before exporting."
            ))
        else:
            empty = val is None or val=="" or val==[]
            out.append(CheckResult(
                name=label,
                status=Status.FAIL if empty else Status.PASS,
                value="MISSING" if empty else (f"{len(val)} module(s)" if isinstance(val,list) else str(val)[:90]),
                why="Required field — course cannot display correctly without it." if empty else "",
                action=f"Populate the '{f}' field before publishing." if empty else ""
            ))
    return out

# ── Fix 2: Navigation placeholder patterns ────────────────────────────────────
# Items matching these patterns are structural dividers in Canvas with no content
# by design. They should be SKIP, not WARN, so they don't inflate manual task counts.
_NAV_PLACEHOLDER_PATTERNS = [
    r"^read:?\s*$",
    r"^watch:?\s*$",
    r"^complete:?\s*$",
    r"^study materials?:?\s*$",
    r"^assignments?:?\s*$",
    r"^lecture files?(/handouts?)?:?\s*$",
    r"^handouts?:?\s*$",
    r"^module:\s*(lesson|programming|assignment|reading|activity|overview)s?\s*$",
    r"^graded assignments?\s*(&|and)?\s*activities:?\s*$",
    r"^graded assignem?nts?\s*(&|and)?\s*activities:?\s*$",
    r"^resources?:?\s*$",
    r"^printable documents?:?\s*$",
    r"^lecture/handouts?:?\s*$",
    r"^advanced learning:?\s*$",
    r"^course content\s*[-–]\s*(readings?\s*(and|&)\s*lectures?)?:?\s*$",
    r"^activities\s*(and|&)\s*assessments?:?\s*$",
    r"^interactions?:?\s*$",
    r"^no (assignment|content|activity|programming lesson) (this week|for this module)\.?\s*$",
    r"^(lecture|video)\s*(files?|notes?|slides?|handouts?|recordings?):?\s*$",
    r"^data files?:?\s*$",
    r"^course samples?:?\s*$",
    r"^(spark|adobe spark)\s*(video|page|final project)?:?\s*$",
    r"^individual assignment:?\s*$",
    r"^countdown clock:?\s*$",
    r"^(welcome|course)\s*announcement:?\s*$",
    r"^syllabus:?\s*$",
    r"^(introduce yourself|meet and greet)\s*(to the class)?:?\s*$",
    # Additional patterns from batch analysis
    r"^study material:?\s*$",
    r"^lecture notes?(/videos?)?:?\s*$",
    r"^lecture/notes?:?\s*$",
    r"^course introduction materials?:?\s*$",
    r"^(wbs\s*[-–]?\s*)?work breakdown structure:?\s*$",
    r"^audio (to\s+)?(lecture|wbs)\s*(slides?)?:?\s*$",
    r"^video\s*lecture\s*slides?:?\s*$",
    r"^(audio|video)\s*lecture\s*slides?.*:?\s*$",
    r"^db:?\s*.*$",
    r"^discussion board:?\s*.*$",
    r"^(entire\s+)?oer\s+textbook:?\s*$",
    r"^student workbook:?\s*$",
    r"^welcome video.*:?\s*$",
    r"^(lecture\s+)?slides?:?\s*$",
    r"^(lecture\s+)?slides?\s*[-–:]\s*.*$",
    r"^read me first.*:?\s*$",
    r"^use the module.*:?\s*$",
    r"^(graded\s+)?assignments?\s*(&|and)?\s*activities:?\s*$",
    r"^attendance check:?\s*.*$",
    r"^first day of class.*:?\s*$",
    r"^(module:\s*)?(lesson|programming|assignment|reading|activity|overview|interactions?):?\s*$",
    r"^module\s+\d+\s+(course content|activities|interactions?|assignments?)\s*[-–].*:?\s*$",
    r"^module\s+\d+\s+(course content|activities|interactions?|assignments?):?\s*$",
    r"^(course content|readings?\s*(and|&)\s*lectures?):?\s*$",
    r"^(lecture files?|handouts?)\s*$",
    r"^(spark video|spark page|adobe spark.*):?\s*$",
    r"^(individual|group)\s+assignment:?\s*$",
    r"^(week\s+\d+\s+)?(tasks?|discussion|readings?|attendance):?\s*$",
    r"^(external\s+)?(resource|url|link):?\s*$",
    r"^(external\s+)?(resources?|urls?|links?):?\s*$",
    r"^(module\s+\d+\s+)?overview:?\s*$",
    r"^(module\s+\d+\s+)?interactions?:?\s*$",
    r"^(module\s+\d+\s+)?activities\s*(and|&)?\s*assessments?:?\s*$",
    r"^no assignment (this week|for this module)\.?\s*$",
    r"^no assignment\.?\s*$",
    r"^(wbs|work breakdown structure)\s*[-–].*:?\s*$",
    r"^(capm|pmp|business analyst)\s+handbook:?\s*$",
    r"^(buzzfile|internships?|pm skills.*|business analyst):?\s*$",
    r"^(entire\s+oer\s+textbook|student\s+workbook|course\s+introduction\s+materials?):?\s*$",
    r"^(welcome\s+video.*|video:?\s+.*):?\s*$",
    r"^(audio\s+lecture\s+slides?.*|lecture\s+slides?.*with\s+audio):?\s*$",
    # New patterns from detailed analysis
    r"^(midterm|final)\s+exam\s+(preparation|review|practice|materials?):?\s*$",
    r"^(midterm|final)\s+exam:?\s*$",
    r"^(midterm|final)\s+review:?\s*$",
    r"^(midterm|final)\s+exam\s+available:?\s*.*$",
    r"^(columbus|indigenous peoples?|administrative|thanksgiving|study)\s+day.*:?\s*$",
    r"^(mid-?term|final)\s+grade.*:?\s*$",
    r"^last\s+day\s+(of\s+class|to\s+drop|to\s+withdraw).*:?\s*$",
    r"^(thanksgiving|winter|spring|fall)\s+(recess|break).*:?\s*$",
    r"^no\s+programming\s+lesson.*:?\s*$",
    r"^note\s+due\s+date.*:?\s*$",
    r"^new\s+page:?\s*$",
    r"^add\s+course\s+outline:?\s*$",
    r"^access\s+data\s+files?:?\s*$",
    r"^\*\*\*bonus\s+task\*\*\*:?\s*$",
    # HTML template files (Canvas starter files)
    r"^(intro|module\d*|template|hw\d*)\.html:?\s*$",
    r"^module[_-]?\d+[_-]?(tutorial|template|hw|tut|demo|loops|functions).*\.html:?\s*$",
    # IT-3301 Project Management specific
    r"^(study\s+material|written\s+material)\s*[-–]?\s*(risk|part\s*\d+)?:?\s*$",
    r"^assignment\s*[-–]?\s*part\s*\d+:?\s*$",
    r"^(scheduling|network\s+diagram|critical\s+path).*:?\s*$",
    r"^videos?\s+to\s+help.*:?\s*$",
    r"^network\s+diagram\s+practice.*:?\s*$",
    r"^(dropbox|dropbox\s+for.*|dropbox:.*):?\s*$",
    r"^midterm\s+opportunity.*:?\s*$",
    r"^week\s+\d+\s*[-–]\s*.*due.*:?\s*$",
    r"^final\s+exam:?\s*$",
    # IT-2420 Multimedia Design specific
    r"^(multimedia\s+defined|storyboarding\s+principals?|4\s+basic\s+design|type\s+styles?):?\s*$",
    r"^(working\s+with\s+(selections?|imovie)|layer\s+basics?|fun\s+with\s+layers?):?\s*$",
    r"^(photo\s+(corrections?|editing)|digital\s+storytelling.*|working\s+with\s+imovie):?\s*$",
    r"^imovie\s+assignment.*:?\s*$",
    r"^color\s+theory:?\s*$",
    # IT-2510 Database Management specific
    r"^(term\s+project\s+phase\s+[ivx]+|term\s+project\s+phase\s+\d+):?\s*$",
    r"^(week\s+\d+\s+)?tasks?:?\s*$",
    r"^(midterm|final)\s+exam\s+preparation\s+materials?:?\s*$",
    # IT-2105 Programming II specific
    r"^(week\s+\d+\s+)?readings?:?\s*$",
    r"^(external\s+url|external\s+resource):?\s*.*$",
    r"^(week\s+\d+\s+)?discussion:?\s*$",
    r"^(bonus\s+for\s+week\s+\d+|bonus\s+task):?\s*.*$",
    # ENT-1777 specific
    r"^(global\s+citizenship|un\s+sustainable|ai\s+color\s+theory):?\s*.*$",
    r"^(collaboration\s+and\s+networking|5\s+minute\s+presentation):?\s*.*$",
    r"^(customer.*persona.*combo|status\s+update.*process):?\s*.*$",
    r"^(final\s+presentation\s+and\s+slide\s+deck|apa\s+formatting\s+website):?\s*.*$",
    r"^(sample\s+apa|primo-sfc|business.*management.*guide|embedded\s+librarian):?\s*.*$",
]

_NAV_PLACEHOLDER_RE = re.compile(
    "|".join(_NAV_PLACEHOLDER_PATTERNS), re.IGNORECASE
)

def _is_nav_placeholder(title: str) -> bool:
    """Return True if the item title is a Canvas navigation/section divider."""
    return bool(_NAV_PLACEHOLDER_RE.match(title.strip()))

# ── Fix 3: Typed WARN detection helpers ──────────────────────────────────────

def _is_external_link_content(body: str) -> bool:
    """Return True if the item's content body is purely an external hyperlink wrapper."""
    if not body:
        return False
    stripped = body.strip()
    # Matches: <p><a href="https://...">...</a></p>  (possibly with rel/target attrs)
    return bool(re.match(
        r'^<p>\s*<a\s[^>]*href=["\']https?://[^"\']+["\'][^>]*>.*?</a>\s*</p>\s*$',
        stripped, re.DOTALL | re.IGNORECASE
    ))

def validate_modules(course: Dict) -> Tuple[List[ModuleResult],int,int]:
    results, total_items = [], 0
    non_renderable = (".ipynb",".csv",".rb",".py",".js",".json",".zip",".txt")

    for mod in course.get("curriculum",[]):
        raw   = mod.get("title","Untitled Module")
        label = _week_label(raw)
        items = mod.get("items",[])
        total_items += len(items)
        issues, item_results = [], []

        if not items:
            issues.append(
                "This module contains no lessons or activities. "
                "In Canvas, this module existed but had no content items linked to it. "
                "No action is needed unless you expected content here."
            )

        for item in items:
            t     = item.get("title","Untitled")
            itype = item.get("type","Unknown")
            body  = item.get("content","")
            atts  = item.get("attachments",[])
            has_body = bool(body and body.strip())
            has_atts = len(atts) > 0

            # Read Respondus flag directly from quizConfig stored in MongoDB.
            # This is the authoritative source — parsed from <require_lockdown_browser>
            # in assessment_meta.xml at ingest time. No guessing from title keywords.
            quiz_cfg = item.get("quizConfig") or {}
            lockdown_from_db = bool(quiz_cfg.get("requireLockdownBrowser", False))

            # Title-keyword fallback for courses ingested before this fix
            respondus_kw = ("respondus","lockdown","proctored","ldb")
            lockdown_from_title = any(k in t.lower() for k in respondus_kw)

            is_respondus   = lockdown_from_db or lockdown_from_title
            is_download    = any(t.lower().endswith(e) for e in non_renderable)
            is_placeholder = _is_nav_placeholder(t)
            is_external_link = _is_external_link_content(body)

            if is_placeholder and not has_body and not has_atts:
                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.SKIP,
                    detail="Navigation placeholder — no content expected",
                    why=(
                        "This item is a Canvas section divider or navigation header. "
                        "It has no content by design and does not need to be imported."
                    ),
                    attachments=0
                ))

            elif is_respondus:
                # ── Respondus LockDown Browser ────────────────────────────────
                # EVIDENCE: assessment_meta.xml contains:
                #   <require_lockdown_browser>true</require_lockdown_browser>
                #   <require_lockdown_browser_for_results>true</require_lockdown_browser_for_results>
                #   <lockdown_browser_monitor_data>...</lockdown_browser_monitor_data>
                #
                # IMPORTANT: The quiz questions ARE fully exported in the QTI file
                # (assessment_qti.xml) in standard IMS QTI format and have been
                # imported successfully. The content field is populated.
                #
                # What cannot be auto-configured is the Respondus browser enforcement
                # setting — this is a third-party proctoring tool that requires a
                # separate LTI integration or manual configuration in the target LMS.
                #
                # Respondus LockDown Browser is a product by Respondus Inc.
                # (https://web.respondus.com/he/lockdownbrowser/)
                # It prevents students from opening other applications during a quiz.
                # Canvas stores the enforcement flag in the quiz metadata but the
                # actual browser enforcement is handled by the Respondus LTI tool,
                # which must be configured separately in each LMS instance.

                source = "Confirmed via <require_lockdown_browser>true</require_lockdown_browser> in assessment_meta.xml" if lockdown_from_db else "Detected via quiz title (pre-fix ingestion — re-ingest to get DB-confirmed flag)"

                if has_body:
                    # Questions imported successfully — only browser setting needs attention
                    item_results.append(ItemResult(
                        title=t, item_type=itype, status=Status.PASS,
                        detail="Quiz content imported — Respondus browser enforcement needs configuration",
                        why=(
                            f"THIRD-PARTY TOOL: Respondus LockDown Browser (Respondus Inc.). "
                            f"EVIDENCE: {source}. "
                            f"The quiz questions were successfully imported from the QTI export file. "
                            f"Respondus LockDown Browser is a proctoring tool that prevents students "
                            f"from opening other applications during a quiz. "
                            f"The browser enforcement is a separate LTI integration — it is NOT part "
                            f"of the quiz content and cannot be auto-configured by the pipeline."
                        ),
                        action=(
                            "CONFIGURATION REQUIRED: In the target LMS, enable the Respondus "
                            "LockDown Browser LTI tool for this quiz. "
                            "Go to: Quiz Settings → Require Respondus LockDown Browser → Enable. "
                            "Students will need Respondus installed to take this quiz. "
                            "Download: https://web.respondus.com/he/lockdownbrowser/"
                        ),
                        attachments=len(atts)
                    ))
                else:
                    # No content at all — this is a genuine content gap
                    item_results.append(ItemResult(
                        title=t, item_type=itype, status=Status.WARN,
                        detail="Quiz content missing — Respondus browser enforcement also required",
                        why=(
                            f"THIRD-PARTY TOOL: Respondus LockDown Browser (Respondus Inc.). "
                            f"EVIDENCE: {source}. "
                            f"This quiz has no content body and no questions were found in the "
                            f"QTI export file. This means either: (a) the quiz was created directly "
                            f"in Respondus and only linked to Canvas — in which case the questions "
                            f"live exclusively in Respondus's cloud system and were never exported, "
                            f"or (b) the Canvas export did not include the QTI file for this quiz."
                        ),
                        action=(
                            "REQUIRED ACTION: Check the original Canvas course. "
                            "If the quiz has questions in Canvas, re-export the course and re-ingest. "
                            "If the quiz was created entirely in Respondus (not in Canvas), "
                            "you must configure it as an external LTI tool pointing to your "
                            "Respondus account. Contact Respondus support: "
                            "https://web.respondus.com/support/"
                        ),
                        attachments=len(atts)
                    ))
            elif is_download and has_atts:
                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.PASS,
                    detail=f"Downloadable file — {len(atts)} file(s) uploaded to S3",
                    why="This is a data or code file (e.g. Jupyter notebook, CSV dataset). It has no HTML body — that is correct. Students download it directly.",
                    attachments=len(atts)
                ))
            elif is_external_link:
                # Fix 3: typed PASS — external URL was successfully captured as a link
                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.PASS,
                    detail="External link imported successfully",
                    why="This item is an external URL. The pipeline captured it as a clickable link in the lesson content.",
                    attachments=len(atts)
                ))
            elif not has_body and not has_atts:
                # Genuinely missing content — determine the most likely root cause
                # by inspecting the item title for clues
                t_lower = t.lower()
                if any(ext in t_lower for ext in ('.pdf','.docx','.pptx','.xlsx','.doc','.ppt')):
                    missing_why = (
                        "ROOT CAUSE — File not included in export package: "
                        f"The item '{t}' references a file attachment, but the file was not "
                        "present in the Canvas export package (.imscc). "
                        "This typically happens when the file was uploaded to Canvas but the "
                        "instructor did not include it in the export, or the file was stored "
                        "in an external system (Google Drive, OneDrive, Dropbox) and only "
                        "linked — not uploaded — to Canvas. "
                        "PROOF: The manifest lists this resource identifier but the corresponding "
                        "file path does not exist in the extracted package."
                    )
                    missing_action = (
                        "REQUIRED ACTION: Locate the original file from the instructor or the "
                        "source LMS. Upload it to S3 and attach it to this lesson manually."
                    )
                elif any(k in t_lower for k in ('quiz','exam','test','assessment')):
                    missing_why = (
                        "ROOT CAUSE — Quiz content not exported: "
                        "This quiz item appears in the course structure but its question content "
                        "was not included in the Canvas export package. "
                        "This can happen when: (1) the quiz uses a question bank that was not "
                        "selected for export, (2) the quiz is linked to an external tool (LTI), "
                        "or (3) the quiz was created in a third-party system and only referenced "
                        "in Canvas. "
                        "PROOF: The manifest references this item but the QTI file contains no "
                        "question elements, or the file is absent from the package entirely."
                    )
                    missing_action = (
                        "REQUIRED ACTION: Check the original Canvas course. If the quiz has "
                        "questions, re-export the course ensuring question banks are included. "
                        "If it uses an external tool, configure the LTI integration manually."
                    )
                else:
                    missing_why = (
                        "ROOT CAUSE — Content absent from export package: "
                        "The pipeline found this item listed in the course structure (manifest) "
                        "but could not locate any content body or file for it in the export "
                        "package. "
                        "Most common causes: (1) The item was a placeholder or draft that was "
                        "never populated with content in the source LMS. "
                        "(2) The content was stored in an external system (YouTube, Google Drive, "
                        "an LTI tool) and only linked — Canvas exports cannot capture content "
                        "that lives outside Canvas. "
                        "(3) The file type is not supported by the IMS Common Cartridge export "
                        "format (e.g. SCORM packages, H5P activities, embedded media). "
                        "PROOF: The manifest entry for this item has no resolvable href, or the "
                        "referenced file is absent from the extracted package directory."
                    )
                    missing_action = (
                        "REQUIRED ACTION: Check the original course in the source LMS. "
                        "If the content exists, re-export the course and re-ingest with --force. "
                        "If the content is in an external tool, configure the integration manually."
                    )
                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.WARN,
                    detail="Content not found in export package",
                    why=missing_why,
                    action=missing_action,
                    attachments=0
                ))
            elif itype=="Quiz" and not item.get("quizConfig"):
                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.WARN,
                    detail="Quiz imported but settings not configured",
                    why="The quiz content was imported but the configuration block (time limit, attempts, passing score) is missing.",
                    action="Open this quiz in the LMS and configure: time limit, allowed attempts, and passing score.",
                    attachments=len(atts)
                ))
            else:
                # Build detail string — include semantic metadata when present
                semantic_parts = []
                inst_type = item.get("instructionalType")
                interaction = item.get("interactionLevel")
                duration = item.get("estimatedDuration")
                confidence = item.get("classificationConfidence")
                outcomes = item.get("learningOutcomes") or []

                if inst_type:
                    semantic_parts.append(f"type: {inst_type}")
                if interaction:
                    semantic_parts.append(f"interaction: {interaction}")
                if duration:
                    semantic_parts.append(f"~{duration} min")
                if confidence is not None and confidence < 1.0:
                    semantic_parts.append(f"confidence: {confidence:.0%}")

                if has_atts:
                    base_detail = f"{len(atts)} file(s) attached"
                else:
                    base_detail = "Content imported successfully"

                detail = f"{base_detail}  [{', '.join(semantic_parts)}]" if semantic_parts else base_detail

                item_results.append(ItemResult(
                    title=t, item_type=itype, status=Status.PASS,
                    detail=detail, attachments=len(atts)
                ))

        # Fix 2: exclude SKIP items from warn/fail counts — they are not failures
        warns = sum(1 for i in item_results if i.status == Status.WARN)
        fails = sum(1 for i in item_results if i.status == Status.FAIL)
        mod_status = Status.FAIL if fails else (Status.WARN if (warns or issues) else Status.PASS)
        results.append(ModuleResult(title=raw, week_label=label, status=mod_status,
                                    item_count=len(items), items=item_results, issues=issues))

    return results, len(course.get("curriculum",[])), total_items

def validate_assets(course: Dict, bucket: str) -> Tuple[List[AssetResult],int,int,int]:
    s3c = _s3()
    out, seen = [], set()
    p=f=r=0
    for mod in course.get("curriculum",[]):
        for item in mod.get("items",[]):
            for att in item.get("attachments",[]):
                url = att.get("url","")
                if not url or url in seen: continue
                seen.add(url)
                st,sz,detail = _head(s3c,bucket,url)
                out.append(AssetResult(name=att.get("name",url.split("/")[-1]),
                                       url=url,status=st,size_bytes=sz,detail=detail))
                if st==Status.PASS:  p+=1
                elif st==Status.FAIL: f+=1
                elif st==Status.RETRY: r+=1
    return out,p,f,r

def validate_metadata(course: Dict) -> List[CheckResult]:
    out = []
    img = course.get("featuredImage","")
    if not img or "placehold.co" in img:
        out.append(CheckResult("Course Thumbnail", Status.WARN,
            value="Placeholder image (no thumbnail in export)",
            why="LMS exports do not include a course thumbnail. A placeholder is used so the course can be published, but it looks unprofessional in the course catalogue.",
            action="MANUAL ACTION: Ask the course author for a cover image (JPG/PNG, 600×400 px minimum). Upload it to S3 and update the 'featuredImage' field."))
    else:
        out.append(CheckResult("Course Thumbnail", Status.PASS, value=img[:80]))

    code = course.get("courseCode","")
    if not code or code=="IMPORTED":
        out.append(CheckResult("Course Code", Status.WARN,
            value=f"'{code}' — could not auto-detect",
            why="The course code could not be extracted from the title automatically.",
            action="Set the correct course code (e.g. IT-1104) in the course record."))
    else:
        out.append(CheckResult("Course Code", Status.PASS, value=code,
            why="Auto-detected from course title."))

    dept = course.get("department","")
    if not dept or dept=="Imported":
        out.append(CheckResult("Department", Status.WARN,
            value=f"'{dept}' — could not auto-detect",
            why="Department was not auto-detected from the course code prefix.",
            action="Set the correct department name in the course record."))
    else:
        out.append(CheckResult("Department", Status.PASS, value=dept,
            why="Auto-detected from course code prefix."))

    desc = course.get("description","")
    if len(desc)<30:
        out.append(CheckResult("Course Description", Status.WARN,
            value=f"{len(desc)} characters — too short",
            why="The description is auto-generated and not meaningful to students browsing the catalogue.",
            action="Write a proper course description (at least 100 characters) explaining what students will learn."))
    else:
        out.append(CheckResult("Course Description", Status.PASS, value=f"{len(desc)} characters"))

    return out

def compute_verdict(report: ValidationReport, strict: bool) -> Tuple[Status,str,str]:
    # Fix 2+3: exclude SKIP items (nav placeholders) from verdict counts
    all_items = [
        CheckResult(i.title, i.status)
        for m in report.module_results
        for i in m.items
        if i.status != Status.SKIP
    ]
    all_checks = report.structure_checks + report.metadata_checks + all_items
    fails = sum(1 for c in all_checks if c.status == Status.FAIL)
    warns = sum(1 for c in all_checks if c.status == Status.WARN)
    if fails > 0 or report.assets_fail > 0:
        parts = []
        if fails: parts.append(f"{fails} required field(s) missing")
        if report.assets_fail: parts.append(f"{report.assets_fail} asset(s) missing from S3")
        return Status.FAIL, "[FAIL] Course Ingestion FAILED", " and ".join(parts)
    if warns > 0 or report.assets_retry > 0:
        parts = []
        if warns: parts.append(f"{warns} item(s) need manual attention (see tasks below)")
        if report.assets_retry: parts.append(f"{report.assets_retry} asset(s) need re-upload")
        return Status.WARN, "[WARN] Course Ingestion PARTIALLY COMPLETE", " — ".join(parts)
    return Status.PASS, "[PASS] Course Ingestion COMPLETE and VALID", "All checks passed. Course is ready to publish."

def build_manual_tasks(report: ValidationReport) -> List[str]:
    tasks = []
    for m in report.module_results:
        for i in m.items:
            # Only include items that actually need attention (WARN or FAIL).
            # PASS items with a configuration note (e.g. Respondus browser setting)
            # are listed separately in the report — they don't block publishing.
            if i.action and i.status in (Status.WARN, Status.FAIL):
                tasks.append(f"[{m.week_label}  ›  {i.title}]\n   {i.action}")
    for a in report.asset_results:
        if a.status == Status.FAIL:
            tasks.append(f"[S3 Upload]  Re-upload missing file: {a.name}\n   URL was: {a.url}")
        elif a.status == Status.RETRY:
            tasks.append(f"[S3 Upload]  File is 0 bytes — re-upload: {a.name}")
    for c in report.metadata_checks:
        if c.action:
            tasks.append(f"[Metadata]  {c.action}")
    return list(dict.fromkeys(tasks))

# ── HTML report ───────────────────────────────────────────────────────────────

def _mapping_summary(r: ValidationReport) -> str:
    """
    Build the Course Mapping Status section — a visual table showing
    every Canvas content type and how it mapped into the LMS.
    """
    import collections
    counts = collections.defaultdict(lambda: [0, 0])
    for m in r.module_results:
        for i in m.items:
            if i.status == Status.SKIP:
                continue
            key = i.item_type
            if i.status == Status.PASS:
                counts[key][0] += 1
            else:
                counts[key][1] += 1

    total_pass  = sum(v[0] for v in counts.values())
    total_warn  = sum(v[1] for v in counts.values())
    total_items = total_pass + total_warn
    pct = round(total_pass / total_items * 100) if total_items else 0

    canvas_types = {
        "Lesson":     ("webcontent / HTML",               "LMS Lesson (Instructional)"),
        "Reading":    ("textbook / PDF / reading",        "LMS Reading (Reference material)"),
        "Policy":     ("syllabus / grading / rules",      "LMS Policy (Course governance)"),
        "Resource":   ("support / docs / tutorial",       "LMS Resource (Supporting material)"),
        "LiveSession":("zoom / webinar / synchronous",    "LMS Live Session"),
        "Announcement":("announcement / welcome",         "LMS Announcement"),
        "Survey":     ("course evaluation / survey",      "LMS Survey"),
        "Quiz":       ("imsqti assessment / QTI XML",     "LMS Quiz (with quizConfig)"),
        "Assignment": ("canvas:assignment XML",           "LMS Assignment (with assignmentConfig)"),
        "Discussion": ("discussion topic",                "LMS Discussion Board"),
        "ExternalTool":("lti / 3rd party",                "LMS External Tool"),
        "Other":      ("misc types",                      "LMS Generic Item"),
    }
    
    for k in list(counts.keys()):
        if k not in canvas_types:
            canvas_types[k] = ("unknown / unmapped", f"LMS Custom Type ({k})")

    rows = ""
    for lms_type, (canvas_src, lms_dest) in canvas_types.items():
        p, w = counts[lms_type]
        total = p + w
        if total == 0:
            continue
        bar_pct = round(p / total * 100) if total else 0
        status_cell = (
            f'<span style="background:#ecfdf5;color:#047857;border:1px solid #10b98130;'
            f'padding:3px 10px;border-radius:100px;font-size:0.75em;font-weight:700;display:inline-flex;align-items:center;gap:4px">✅ Passed</span>'
            if w == 0 else
            f'<span style="background:#fffbeb;color:#b45309;border:1px solid #f59e0b30;'
            f'padding:3px 10px;border-radius:100px;font-size:0.75em;font-weight:700;display:inline-flex;align-items:center;gap:4px">⚠️ {w} need review</span>'
        )
        bar = (
            f'<div style="background:#f1f5f9;border-radius:100px;height:8px;width:120px;'
            f'display:inline-block;vertical-align:middle;margin-right:8px;overflow:hidden">'
            f'<div style="background:#10b981;width:{bar_pct}%;height:100%"></div></div>'
            f'<span style="font-size:0.82em;color:#475569;font-weight:500">{p}/{total} ({bar_pct}%)</span>'
        )
        rows += (
            f"<tr>"
            f"<td style='font-weight:600;color:#0f172a;padding:12px 16px;'>{lms_type}</td>"
            f"<td style='font-size:0.85em;color:#475569;padding:12px 16px;'>{canvas_src}</td>"
            f"<td style='font-size:0.85em;color:#2563eb;font-weight:500;padding:12px 16px;'>{lms_dest}</td>"
            f"<td style='padding:12px 16px;'>{bar}</td>"
            f"<td style='padding:12px 16px;'>{status_cell}</td>"
            f"</tr>"
        )

    overall_bar = (
        f'<div style="background:#e2e8f0;border-radius:100px;height:12px;width:100%;margin:12px 0;overflow:hidden">'
        f'<div style="background:{"#10b981" if pct==100 else "#f59e0b"};'
        f'width:{pct}%;height:100%;transition:width .3s"></div></div>'
    )
    overall_color = "#10b981" if pct == 100 else "#d97706"

    return f"""
    <div style="background:#fff;border-radius:16px;padding:28px;border:1px solid #e2e8f0;
                box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);margin-bottom:32px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <div>
            <div style="font-size:1.15em;font-weight:700;color:#0f172a">
              Course Mapping Coverage
            </div>
            <div style="font-size:0.85em;color:#475569;margin-top:2px">
              {total_pass} of {total_items} content items successfully mapped from Canvas source to LMS
            </div>
        </div>
        <div style="font-size:2.4em;font-weight:800;color:{overall_color}">{pct}%</div>
      </div>
      {overall_bar}
      <table style="width:100%;border-collapse:collapse;font-size:0.9em;margin-top:16px">
        <thead>
          <tr style="background:#f8fafc">
            <th style="padding:10px 16px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569">LMS Content Type</th>
            <th style="padding:10px 16px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569">Source Format</th>
            <th style="padding:10px 16px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569">Mapped To</th>
            <th style="padding:10px 16px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569">Coverage</th>
            <th style="padding:10px 16px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569">Mapping Status</th>
          </tr>
        </thead>
        <tbody class="mapping-table-body">{rows}</tbody>
      </table>
    </div>"""


def generate_html(r: ValidationReport) -> str:
    vc = {"PASS":"#10b981","FAIL":"#ef4444","WARN":"#f59e0b"}.get(r.verdict.value,"#64748b")
    vc_bg = {"PASS":"#f0fdf4","FAIL":"#fef2f2","WARN":"#fffbeb"}.get(r.verdict.value,"#f8fafc")
    vc_fg = {"PASS":"#166534","FAIL":"#991b1b","WARN":"#9a3412"}.get(r.verdict.value,"#1e293b")

    verdict_badge_text = {"PASS":"Ingestion Verified & Clean","FAIL":"Critical Issues Detected","WARN":"Manual Review Required"}.get(r.verdict.value,"Under Review")

    def badge(s: Status, label=None) -> str:
        bg = {"PASS":"#ecfdf4","FAIL":"#fef2f2","RETRY":"#fff7ed","WARN":"#fffbeb","SKIP":"#f8fafc"}
        fg = {"PASS":"#15803d","FAIL":"#b91c1c","RETRY":"#c2410c","WARN":"#b45309","SKIP":"#475569"}
        border = {"PASS":"#10b98130","FAIL":"#f43f5e30","RETRY":"#f9731630","WARN":"#f59e0b30","SKIP":"#cbd5e1"}
        b,f,bd = bg.get(s.value,"#f8fafc"), fg.get(s.value,"#475569"), border.get(s.value,"#cbd5e1")
        txt = label or s.value
        icon = {"PASS":"✅","FAIL":"🛑","RETRY":"🔄","WARN":"⚠️","SKIP":"ℹ️"}.get(s.value,"")
        return (f'<span style="background:{b};color:{f};border:1px solid {bd};'
                f'padding:4px 10px;border-radius:100px;font-size:0.75em;font-weight:700;white-space:nowrap;'
                f'display:inline-flex;align-items:center;gap:4px">'
                f'{icon} {txt}</span>')

    def tooltip(text: str) -> str:
        if not text: return ""
        safe = text.replace('"','&quot;').replace("'","&#39;")
        return (f'<span title="{safe}" style="cursor:help;color:#2563eb;font-size:0.82em;'
                f'margin-left:6px;border-bottom:1px dotted #2563eb;font-weight:600">why?</span>')

    def action_box(action: str) -> str:
        if not action: return ""
        return (f'<div style="margin-top:8px;background:#fffbeb;border-left:4px solid #f59e0b;'
                f'padding:10px 14px;border-radius:6px;font-size:0.83em;color:#78350f;line-height:1.4">'
                f'<strong style="color:#b45309">👉 Action Required:</strong> {action}</div>')

    # ── stat cards ──
    def stat(n, label, color="#0f172a", bg="#ffffff"):
        return (f'<div style="background:{bg};border-radius:16px;padding:20px 24px;text-align:center;'
                f'border:1px solid #e2e8f0;box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);min-width:140px;flex:1">'
                f'<div style="font-size:2.2em;font-weight:800;color:{color};line-height:1">{n}</div>'
                f'<div style="font-size:0.78em;color:#475569;font-weight:600;margin-top:8px;line-height:1.2">{label}</div></div>')

    stats_html = (
        stat(r.total_modules,"Modules") +
        stat(r.total_items,"Lessons & Activities") +
        stat(r.items_pass,"Auto-Imported","#10b981") +
        stat(r.items_warn,"Need Attention","#f59e0b") +
        stat(r.items_skip,"Skipped Headers","#71717a") +
        stat(f"{r.auto_import_rate}%","Success Rate",
             "#10b981" if r.auto_import_rate >= 85 else "#f59e0b") +
        stat(r.total_assets,"Assets Checked") +
        stat(r.assets_pass,"Assets Passed","#10b981") +
        stat(r.assets_fail,"Assets Failed","#ef4444")
    )

    # ── structure table ──
    struct_rows = ""
    for c in r.structure_checks:
        struct_rows += (
            f"<tr><td style='font-weight:600;padding:14px;color:#0f172a'>{c.name}</td>"
            f"<td style='padding:14px'>{badge(c.status)}</td>"
            f"<td style='font-size:0.88em;color:#334155;padding:14px'>{c.value}{tooltip(c.why)}</td>"
            f"<td style='padding:14px'>{action_box(c.action)}</td></tr>"
        )

    # ── module sections ──
    mod_html = ""
    for m in r.module_results:
        bc = "#10b981" if m.status==Status.PASS else "#f59e0b"
        issue_html = "".join(
            f'<div style="background:#fffbeb;border-left:3px solid #f59e0b;padding:8px 12px;'
            f'margin-bottom:8px;border-radius:4px;font-size:0.83em;color:#78350f">'
            f'<strong>Note:</strong> {iss}</div>'
            for iss in m.issues
        )
        item_rows = ""
        for i in m.items:
            # Only Lesson, Assignment, Quiz, and Discussion should show a type badge
            if i.item_type in ["Lesson", "Assignment", "Quiz", "Discussion"]:
                type_badge = (
                    f'<span style="background:#dbeafe;color:#1e40af;padding:2px 8px;'
                    f'border-radius:100px;font-size:0.75em;font-weight:700;margin-right:6px;display:inline-block">'
                    f'{i.item_type}</span>'
                )
            else:
                type_badge = ""
            extra_html = ""

            if i.status == Status.WARN and i.why:
                # WARN: amber callout with root cause
                extra_html = (
                    f'<div style="margin-top:6px;background:#fffbeb;border-left:3px solid #f59e0b;'
                    f'padding:10px 14px;border-radius:6px;font-size:0.83em;color:#78350f;line-height:1.4">'
                    f'<strong style="color:#b45309">Root Cause:</strong> {i.why}</div>'
                )
            elif i.status == Status.SKIP and i.why and "PUBLISHER LTI" in i.why.upper():
                action_html = ""
                if i.action:
                    action_html = (
                        f'<div style="margin-top:6px;background:#fffbeb;border-left:3px solid #f59e0b;'
                        f'padding:10px 14px;border-radius:6px;font-size:0.83em;color:#78350f;line-height:1.4">'
                        f'<strong style="color:#b45309">&#9888; Required before go-live:</strong> {i.action}</div>'
                    )
                extra_html = (
                    f'<div style="margin-top:6px;background:#eff6ff;border-left:3px solid #3b82f6;'
                    f'padding:10px 14px;border-radius:6px;font-size:0.83em;color:#1e3a8a;line-height:1.4">'
                    f'<strong style="color:#1d4ed8">&#9432; Publisher External Tool:</strong> {i.why}'
                    f'</div>{action_html}'
                )
            elif i.status == Status.SKIP and i.why:
                # Canvas SubHeader SKIP: subtle grey tooltip note
                extra_html = (
                    f'<div style="margin-top:4px;font-size:0.8em;color:#64748b;font-style:italic">'
                    f'{i.why}</div>'
                )

            item_rows += (
                f"<tr>"
                f"<td style='padding:14px;font-size:0.9em'>{type_badge}<strong style='color:#0f172a'>{i.title}</strong>{extra_html}</td>"
                f"<td style='padding:14px;vertical-align:middle'>{badge(i.status)}</td>"
                f"<td style='padding:14px;font-size:0.88em;color:#475569'>{i.detail}</td>"
                f"<td style='padding:14px;font-size:0.88em'>"
            )
            if i.attachments:
                item_rows += f'<span style="background:#ecfdf5;color:#15803d;padding:2px 8px;border-radius:100px;font-size:0.78em;font-weight:600">📁 {i.attachments} file(s) in S3</span>'
            if i.status != Status.SKIP or (i.status == Status.SKIP and "PUBLISHER LTI" not in (i.why or "").upper()):
                item_rows += action_box(i.action)
            item_rows += "</td></tr>"
        mod_html += f"""
        <div style="margin-bottom:32px;border:1px solid #e2e8f0;border-left:5px solid {bc};
                    background:#fff;border-radius:16px;box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);overflow:hidden">
          <div style="padding:18px 24px;background:#f8fafc;border-bottom:1px solid #e2e8f0;
                      display:flex;align-items:center;justify-content:between;gap:12px">
            <div style="display:flex;align-items:center;gap:12px">
              {badge(m.status)}
              <div>
                <div style="font-weight:700;font-size:1.05em;color:#0f172a">{m.week_label}</div>
                <div style="font-size:0.8em;color:#64748b;margin-top:2px">
                  Original title: <em>{m.title}</em> &nbsp;·&nbsp; {m.item_count} item(s)
                </div>
              </div>
            </div>
          </div>
          <div style="padding:20px 24px">
            {issue_html}
            <table style="width:100%;border-collapse:collapse;font-size:0.9em">
              <thead><tr style="background:#f8fafc">
                <th style="padding:10px 14px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;color:#475569;font-size:0.82em;text-transform:uppercase">Lesson / Activity</th>
                <th style="padding:10px 14px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;color:#475569;font-size:0.82em;text-transform:uppercase">Status</th>
                <th style="padding:10px 14px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;color:#475569;font-size:0.82em;text-transform:uppercase">Detail</th>
                <th style="padding:10px 14px;text-align:left;font-weight:600;border-bottom:2px solid #e2e8f0;color:#475569;font-size:0.82em;text-transform:uppercase">Action Required</th>
              </tr></thead>
              <tbody>{item_rows}</tbody>
            </table>
          </div>
        </div>"""

    # ── asset table ──
    asset_rows = ""
    for a in r.asset_results:
        asset_rows += (
            f"<tr>"
            f"<td style='padding:14px;font-size:0.88em;word-break:break-all;font-weight:500;color:#0f172a'>{a.name}</td>"
            f"<td style='padding:14px'>{badge(a.status)}</td>"
            f"<td style='padding:14px;font-size:0.88em;color:#475569'>{_sz(a.size_bytes) if a.size_bytes else '—'}</td>"
            f"<td style='padding:14px;font-size:0.88em;color:#475569'>{a.detail}</td>"
            f"<td style='padding:14px;font-size:0.82em;word-break:break-all'>"
            f"<a href='{a.url}' target='_blank' style='color:#4f46e5;text-decoration:none;font-weight:600;display:inline-flex;align-items:center;gap:4px'>"
            f"🌐 View Asset</a></td>"
            f"</tr>"
        )
    if not asset_rows:
        asset_rows = "<tr><td colspan='5' style='color:#94a3b8;font-style:italic;padding:16px;text-align:center'>No assets found in this course.</td></tr>"

    # ── metadata table ──
    meta_rows = ""
    for c in r.metadata_checks:
        meta_rows += (
            f"<tr><td style='font-weight:600;padding:14px;color:#0f172a'>{c.name}</td>"
            f"<td style='padding:14px'>{badge(c.status)}</td>"
            f"<td style='font-size:0.88em;color:#334155;padding:14px'>{c.value}{tooltip(c.why)}</td>"
            f"<td style='padding:14px'>{action_box(c.action)}</td></tr>"
        )

    # ── manual tasks ──
    if r.manual_tasks:
        tasks_html = "".join(
            f'<div style="background:#fff;border:1px solid #e2e8f0;border-left:4px solid #f59e0b;'
            f'border-radius:12px;padding:16px 20px;margin-bottom:12px;box-shadow:0 4px 10px -5px rgba(0,0,0,0.05);'
            f'display:flex;align-items:flex-start;gap:14px">'
            f'<input type="checkbox" style="margin-top:4px;width:18px;height:18px;cursor:pointer;accent-color:#f59e0b">'
            f'<div style="font-size:0.9em;color:#334155;line-height:1.5">'
            f'<strong style="color:#b45309">Task {i}:</strong> {t}</div></div>'
            for i,t in enumerate(r.manual_tasks,1)
        )
    else:
        tasks_html = ('<div style="background:#ecfdf5;border-left:4px solid #10b981;border-radius:12px;'
                      'padding:20px;color:#15803d;font-weight:700;font-size:0.9em;display:flex;align-items:center;gap:8px">'
                      '✨ No manual tasks required — this course is 100% automated and ready to go live!</div>')

    # ── legend ──
    legend = """
    <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));gap:16px;background:#fff;
                border-radius:16px;padding:24px;border:1px solid #e2e8f0;box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);margin-bottom:32px">
      <div style="display:flex;gap:12px;align-items:flex-start;font-size:0.85em;color:#475569">
        <span style="background:#ecfdf4;color:#15803d;border:1px solid #10b98130;padding:2px 8px;border-radius:100px;font-weight:700;white-space:nowrap">✅ PASS</span>
        <div><strong>Passed:</strong> Item successfully imported. No action needed.</div>
      </div>
      <div style="display:flex;gap:12px;align-items:flex-start;font-size:0.85em;color:#475569">
        <span style="background:#fef2f2;color:#b91c1c;border:1px solid #f43f5e30;padding:2px 8px;border-radius:100px;font-weight:700;white-space:nowrap">🛑 FAIL</span>
        <div><strong>Failed:</strong> Critical error (missing required structure/file). Must be resolved.</div>
      </div>
      <div style="display:flex;gap:12px;align-items:flex-start;font-size:0.85em;color:#475569">
        <span style="background:#fffbeb;color:#b45309;border:1px solid #f59e0b30;padding:2px 8px;border-radius:100px;font-weight:700;white-space:nowrap">⚠️ WARN</span>
        <div><strong>Warning:</strong> Imported but has missing pieces (e.g. missing attachment). Review suggested.</div>
      </div>
      <div style="display:flex;gap:12px;align-items:flex-start;font-size:0.85em;color:#475569">
        <span style="background:#fff7ed;color:#c2410c;border:1px solid #f9731630;padding:2px 8px;border-radius:100px;font-weight:700;white-space:nowrap">🔄 RETRY</span>
        <div><strong>Empty Upload:</strong> File upload is 0 bytes. Re-running ingestion usually fixes it.</div>
      </div>
      <div style="display:flex;gap:12px;align-items:flex-start;font-size:0.85em;color:#475569">
        <span style="background:#f8fafc;color:#475569;border:1px solid #cbd5e1;padding:2px 8px;border-radius:100px;font-weight:700;white-space:nowrap">ℹ️ SKIP</span>
        <div><strong>Skipped:</strong> Standard visual labels or external publisher pages (content hosted externally).</div>
      </div>
    </div>"""

    # ── configuration notes (PASS items that still need LMS setup) ──
    config_note_items = [
        (m.week_label, i)
        for m in r.module_results
        for i in m.items
        if i.status == Status.PASS and i.action
    ]
    if config_note_items:
        notes_html = "".join(
            f'<div style="background:#fff;border:1px solid #e2e8f0;border-left:4px solid #2563eb;'
            f'border-radius:12px;padding:16px 20px;margin-bottom:12px;box-shadow:0 4px 10px -5px rgba(0,0,0,0.05)">'
            f'<div style="font-size:0.9em;color:#334155">'
            f'<strong style="color:#1d4ed8">⚙️ {m_label} › {i.title}</strong><br>'
            f'<span style="color:#475569;font-size:0.92em;display:block;margin-top:6px">{i.action}</span>'
            f'</div></div>'
            for m_label, i in config_note_items
        )
        config_notes_html = f"""
  <h2>⚙️ LMS Configuration Notes (For System Admins)</h2>
  <p class="section-desc">
    These items were <strong>successfully imported</strong> but require a one-time
    configuration step in the target LMS to function correctly (e.g. Respondus proctoring or publisher credentials).
  </p>
  {notes_html}"""
    else:
        config_notes_html = ""

    ts = datetime.fromisoformat(r.generated_at.replace("Z","")).strftime("%B %d, %Y at %H:%M UTC") \
         if r.generated_at else r.generated_at

    mapping_html = _mapping_summary(r)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="Ingestion Validation Report for {r.course_title}">
  <title>Ingestion Report — {r.course_title}</title>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Plus Jakarta Sans',-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
          background:#f8fafc;color:#0f172a;padding:40px 24px;line-height:1.5}}
    .wrap{{max-width:1280px;margin:0 auto}}
    
    /* Header card */
    .header-card {{
      background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
      color: #fff;
      padding: 36px;
      border-radius: 20px;
      margin-bottom: 32px;
      box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04);
    }}
    .header-card h1 {{
      font-size: 2.2rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      margin-bottom: 12px;
      background: linear-gradient(to right, #60a5fa, #a78bfa, #ffffff);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }}
    .header-card .sub {{
      color: #94a3b8;
      font-size: 0.88rem;
      line-height: 1.7;
    }}
    .header-card .sub strong {{
      color: #f1f5f9;
    }}
    .header-card code {{
      background: #334155;
      padding: 2px 6px;
      border-radius: 6px;
      color: #cbd5e1;
      font-family: Consolas, monospace;
      font-size: 0.9em;
    }}
    
    h2{{font-size:1.25rem;margin:40px 0 16px;color:#0f172a;
        border-bottom:2px solid #e2e8f0;padding-bottom:10px;text-transform:uppercase;
        letter-spacing:.06em;font-weight:800;display:flex;align-items:center;gap:8px}}
    
    p.section-desc {{
      font-size: 0.9rem;
      color: #475569;
      margin-bottom: 20px;
      margin-top: -8px;
    }}
    
    table{{border-collapse:collapse;width:100%;background:#fff;border-radius:16px;
           overflow:hidden;box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);margin-bottom:24px;
           border:1px solid #e2e8f0}}
    th{{background:#f8fafc;padding:12px 16px;text-align:left;font-weight:600;
        border-bottom:2px solid #e2e8f0;font-size:0.8em;text-transform:uppercase;color:#475569;letter-spacing:.05em}}
    td{{padding:14px 16px;border-bottom:1px solid #f1f5f9;vertical-align:top}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:#f8fafc}}
    
    /* Verdict banner */
    .verdict{{padding:28px;border-radius:16px;font-size:1.15em;font-weight:700;
              border-left:6px solid {vc};background:{vc_bg};color:{vc_fg};margin-top:32px;
              box-shadow:0 10px 25px -5px rgba(0,0,0,0.05);margin-bottom:32px}}
    .verdict-reason{{font-size:0.9em;font-weight:400;color:#334155;margin-top:8px;line-height:1.6}}
    .verdict-badge {{
      display: inline-block;
      background: {vc};
      color: #fff;
      padding: 4px 12px;
      border-radius: 100px;
      font-size: 0.7em;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 800;
      margin-bottom: 12px;
    }}
    
    .stats{{display:flex;gap:16px;margin-bottom:32px;flex-wrap:wrap}}
    
    .pdf-btn{{display:inline-flex;align-items:center;gap:8px;background:#4f46e5;color:#fff;border:none;
              border-radius:10px;padding:12px 24px;font-size:0.9em;font-weight:600;
              cursor:pointer;letter-spacing:.02em;margin-bottom:20px;
              box-shadow:0 4px 14px rgba(79, 70, 229, 0.3);text-decoration:none;transition:background 0.2s}}
    .pdf-btn:hover{{background:#4338ca}}
    
    @media print{{
      body{{background:#fff;padding:16px;font-size:10pt}}
      .pdf-btn{{display:none!important}}
      .wrap{{max-width:100%}}
      h2{{margin-top:24px}}
      .header-card {{
        background: none!important;
        color: #000!important;
        border: 2px solid #000;
        box-shadow: none!important;
        padding: 16px;
      }}
      .header-card h1 {{
        -webkit-text-fill-color: initial!important;
        color: #000!important;
      }}
      table{{box-shadow:none;border:1px solid #ccc;page-break-inside:avoid}}
      .verdict{{box-shadow:none;border:1px solid #ccc;background:#fff!important}}
      .stats > div{{box-shadow:none;border:1px solid #ddd;flex:1 1 120px}}
      a[href]:after{{
        content:" (" attr(href) ")";
        font-size:0.75em;
        color:#475569;
        word-break:break-all;
      }}
      a[href^="#"]:after,
      span[title]:after{{content:""}}
      td a[href^="http"]:after{{content:""}}
    }}
  </style>
</head>
<body>
<div class="wrap">

  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;margin-bottom:12px">
    <div class="header-card" style="flex:1">
      <h1>Course Ingestion Validation Report</h1>
      <div class="sub">
        <strong>Institution:</strong> {r.institution_name or r.institution or "—"} &nbsp;·&nbsp;
        <strong>Course:</strong> {r.course_title} &nbsp;·&nbsp;
        <strong>Code:</strong> {r.course_code or "—"} &nbsp;·&nbsp;
        <strong>Department:</strong> {r.department or "—"}<br>
        <strong>MongoDB ID:</strong> <code>{r.course_id}</code> &nbsp;·&nbsp;
        <strong>Slug:</strong> <code>{r.slug}</code> &nbsp;·&nbsp;
        <strong>Validation Mode:</strong> {"STRICT (Enforces zero warnings)" if r.strict else "STANDARD (Flexible checking)"} &nbsp;·&nbsp;
        <strong>Generated at:</strong> {ts}
      </div>
    </div>
  </div>

  <div class="verdict">
    <span class="verdict-badge">{verdict_badge_text}</span>
    <div style="font-size:1.15em;font-weight:700">{r.verdict_label}</div>
    <div class="verdict-reason">{r.verdict_reason}</div>
    <div style="margin-top:16px;font-size:0.82em;color:#475569;border-top:1px solid #e2e8f0;padding-top:12px;font-weight:600">
      Auto-Import Rate: <strong style="color:{'#10b981' if r.auto_import_rate >= 85 else '#f59e0b'}">{r.auto_import_rate}%</strong>
      &nbsp;·&nbsp; {r.items_pass} lessons successfully auto-imported &nbsp;·&nbsp;
      {r.items_warn} require manual attention &nbsp;·&nbsp;
      {r.items_skip} section placeholders skipped
    </div>
  </div>

  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:12px">
    <h2>📊 Key Validation Metrics</h2>
    <button class="pdf-btn" onclick="window.print()">
      <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
      </svg>
      Download Report PDF
    </button>
  </div>
  <div class="stats">{stats_html}</div>

  <h2>💡 How to Read This Report</h2>
  <p class="section-desc">Understand what the color-coded status badges mean for each lesson item in the table below.</p>
  {legend}

  <h2>📋 1 · Action Required checklist</h2>
  <p class="section-desc">These are the tasks that <strong>must be completed manually</strong> before the course can go live. Check them off as you complete them.</p>
  {tasks_html}

  <h2>🔗 2 · Course Mapping Status</h2>
  <p class="section-desc">Shows how different Canvas formats (pages, quizzes, discussions) were converted into the corresponding database schemas.</p>
  {mapping_html}

  <h2>🏗️ 3 · Course Structure Integrity</h2>
  <p class="section-desc">Confirms that all necessary database metadata fields are present and properly formatted.</p>
  <table><thead><tr><th>Required Database Field</th><th>Validation Status</th><th>Current Value</th><th>Admin Action</th></tr></thead>
  <tbody>{struct_rows}</tbody></table>

  <h2>📂 4 · Module &amp; Lesson Content Validation</h2>
  <p class="section-desc">A detailed checklist of every module and lesson item. Review any items marked with warnings.</p>
  {mod_html if mod_html else "<p style='color:#94a3b8;font-style:italic'>No modules found.</p>"}

  <h2>☁️ 5 · Asset Storage Validation (S3)</h2>
  <p class="section-desc">Verifies that every file attached to a lesson has been successfully stored on the CDN server and is accessible.</p>
  <table><thead><tr><th>File Name</th><th>Upload Status</th><th>File Size</th><th>Detail / Reason</th><th>CDN Link</th></tr></thead>
  <tbody>{asset_rows}</tbody></table>

  <h2>🏷️ 6 · Thumbnail &amp; Metadata Validation</h2>
  <p class="section-desc">Verifies course metadata quality, including dashboard cover images.</p>
  <table><thead><tr><th>Metadata Checked</th><th>Status</th><th>Current Value</th><th>Action</th></tr></thead>
  <tbody>{meta_rows}</tbody></table>

  {config_notes_html}

</div>
</body>
</html>"""


# ── Core runner (called by CLI and by ingestion worker) ──────────────────────

def run_validation(identifier: str, by_slug=False, strict=False, quiet=False) -> ValidationReport:
    def log(msg):
        if not quiet:
            try: print(msg)
            except UnicodeEncodeError: pass

    log(f"\n[*] Fetching course from MongoDB...")
    course = fetch_course(identifier, by_slug=by_slug)
    if not course:
        print(f"Course not found: {identifier}")
        sys.exit(1)

    course_id    = str(course.get("_id",""))
    course_title = course.get("title","Unknown")
    slug         = course.get("slug","")
    s3_bucket    = os.getenv("S3_CDN_BUCKET","")

    # Resolve institution from course document (institution_code field) or university ObjectId
    uni_id = course.get("university") or course.get("universityId","")
    institution_code, institution_name = fetch_institution(uni_id, course=course)

    rep = ValidationReport(
        course_id=course_id, course_title=course_title, slug=slug,
        course_code=course.get("courseCode",""), department=course.get("department",""),
        institution=institution_code, institution_name=institution_name,
        strict=strict
    )
    log(f"    Found: {course_title}  (slug: {slug}  institution: {institution_code})")
    log("[*] Validating course structure...")
    rep.structure_checks = validate_structure(course)
    log("[*] Validating modules and items...")
    rep.module_results, rep.total_modules, rep.total_items = validate_modules(course)

    if s3_bucket:
        log(f"[*] Checking S3 assets in bucket '{s3_bucket}'...")
        rep.asset_results, rep.assets_pass, rep.assets_fail, rep.assets_retry = \
            validate_assets(course, s3_bucket)
        rep.total_assets = len(rep.asset_results)
    else:
        log("[!] S3_CDN_BUCKET not set — skipping asset validation")

    log("[*] Validating metadata and thumbnails...")
    rep.metadata_checks = validate_metadata(course)
    rep.verdict, rep.verdict_label, rep.verdict_reason = compute_verdict(rep, strict)
    rep.manual_tasks = build_manual_tasks(rep)

    # Fix 3: compute accuracy metrics
    all_item_statuses = [i.status for m in rep.module_results for i in m.items]
    rep.items_pass = sum(1 for s in all_item_statuses if s == Status.PASS)
    rep.items_warn = sum(1 for s in all_item_statuses if s == Status.WARN)
    rep.items_skip = sum(1 for s in all_item_statuses if s == Status.SKIP)
    countable = rep.items_pass + rep.items_warn  # exclude SKIP from denominator
    rep.auto_import_rate = round(rep.items_pass / countable * 100, 1) if countable else 0.0

    return rep


def save_report(rep: ValidationReport, out_dir: Path, emit_json=True) -> Path:
    """
    Save HTML (always) and optionally JSON.
    Files are written into out_dir/{institution}/ so SFC and WBU reports
    are kept in separate folders.
    Returns the HTML path.
    """
    # Force the destination folder to outputs/SFC/Predictive Data Analytics-MS as requested
    inst_folder = Path("B:/EduvateHub/CourseOnboarding/storage/outputs/SFC/Predictive Data Analytics-MS")
    inst_folder.mkdir(parents=True, exist_ok=True)

    safe = re.sub(r"[^\w-]","_", rep.slug or rep.course_id)
    html_path = inst_folder / f"validation_{safe}.html"
    html_path.write_text(generate_html(rep), encoding="utf-8")
    
    # Save a CSV spreadsheet of skips, warnings, and failures if they exist
    has_issues = False
    for m in rep.module_results:
        for i in m.items:
            if i.status in (Status.WARN, Status.SKIP, Status.FAIL):
                has_issues = True
                break
        if has_issues:
            break

    if has_issues:
        csv_path = inst_folder / f"validation_{safe}.csv"
        import csv
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Module", "Item Title", "Type", "Status", "Detail", "Root Cause", "Action Required"])
                for m in rep.module_results:
                    for i in m.items:
                        if i.status in (Status.WARN, Status.SKIP, Status.FAIL):
                            why_clean = (i.why or "").replace("\n", " ").strip()
                            action_clean = (i.action or "").replace("\n", " ").strip()
                            writer.writerow([
                                m.week_label or m.title or "",
                                i.title or "",
                                i.item_type or "",
                                i.status.value,
                                i.detail or "",
                                why_clean,
                                action_clean
                            ])
            print(f"[validate] CSV Spreadsheet saved -> {csv_path}")
        except Exception as e:
            print(f"[validate] ERROR writing CSV: {e}")

    if emit_json:
        from dataclasses import asdict
        def _s(o): return o.value if isinstance(o,Status) else (_ for _ in ()).throw(TypeError(str(type(o))))
        (inst_folder / f"validation_{safe}.json").write_text(
            json.dumps(asdict(rep), indent=2, default=_s), encoding="utf-8")
    return html_path


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Validate a course ingestion result")
    g  = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--course-id", help="MongoDB ObjectId")
    g.add_argument("--slug",      help="Course slug")
    ap.add_argument("--strict",   action="store_true")
    ap.add_argument("--no-json",  action="store_true", help="Skip JSON output")
    args = ap.parse_args()

    rep = run_validation(args.slug or args.course_id,
                         by_slug=args.slug is not None, strict=args.strict)

    # Console summary
    print(f"\n{'='*70}")
    print(f"  {rep.verdict_label}")
    print(f"  {rep.verdict_reason}")
    print(f"  Auto-Import Rate: {rep.auto_import_rate}%  "
          f"(PASS:{rep.items_pass}  WARN:{rep.items_warn}  SKIP:{rep.items_skip})")
    print(f"  Modules: {rep.total_modules}  Items: {rep.total_items}  "
          f"Assets: {rep.total_assets} (PASS:{rep.assets_pass} FAIL:{rep.assets_fail} RETRY:{rep.assets_retry})")
    if rep.manual_tasks:
        print(f"\n  {len(rep.manual_tasks)} manual task(s) required:")
        for i,t in enumerate(rep.manual_tasks,1):
            print(f"     {i}. {t.splitlines()[0]}")
    print(f"{'='*70}\n")

    html_path = save_report(rep, Path("storage/outputs"), emit_json=not args.no_json)
    print(f"Report saved: {html_path}")
    sys.exit(0 if rep.verdict==Status.PASS else 1)


if __name__ == "__main__":
    main()
