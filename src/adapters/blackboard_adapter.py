"""
Blackboard Learn Ultra Export Adapter

Parses Blackboard course export packages (.zip / .imscc exported from
wbu.blackboard.com or any Blackboard Learn instance) and converts them
into the same CanvasCourse intermediate model that the rest of the
pipeline already understands.

Key differences from Canvas IMS-CC that this adapter handles:

1. Manifest namespace
   Canvas  : xmlns="http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1"
   BB      : xmlns:bb="http://www.blackboard.com/content-packaging/"
             Resources use bb:file="resNNNNN.dat" instead of href="..."

2. Content files
   Canvas  : wiki_content/*.html  or  web_resources/**/*.html
   BB      : resNNNNN.dat at the package root — XML with a <CONTENT> root

3. The ultraDocumentBody pattern (Blackboard Ultra)
   Every visible item is a FOLDER (<CONTENTHANDLER value="resource/x-bb-folder"/>)
   whose actual HTML lives in a child item titled "ultraDocumentBody".
   The adapter collapses parent+child into a single lesson.

4. Body encoding
   BB stores HTML inside <BODY><TEXT> as an HTML-escaped string.
   e.g.  &lt;p&gt;Hello&lt;/p&gt;  →  <p>Hello</p>
   The adapter unescapes it before storing.

5. Assessments
   BB uses Blackboard QTI (.dat files with <questestinterop> root and
   <bbmd_*> metadata extensions).  The adapter maps them to CanvasQuiz
   objects with questions extracted from <item> elements.

6. Discussions
   BB uses <FORUM> XML with <MESSAGETHREADS><MSG> for the prompt text.
   The adapter maps them to CanvasDiscussion objects.

7. Course title
   The manifest title is the internal TOC name ("ROOT").
   The real title lives in the course-settings .dat file
   (type="course/x-bb-coursesetting") as <TITLE value="..."/>.
"""

import html
import re
import tempfile
import shutil
import zipfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from models.canvas_models import (
    CanvasCourse, CanvasModule, CanvasModuleItem,
    CanvasPage, CanvasQuiz, CanvasAssignment, CanvasDiscussion,
    CanvasResource, WorkflowState,
)
from observability.logger import get_logger
from utils.zip_utils import safe_extractall

logger = get_logger(__name__)

# ── XML helpers ───────────────────────────────────────────────────────────────

def _parse_xml(path: Path):
    """Parse an XML file, return root element or None on failure."""
    try:
        import xml.etree.ElementTree as ET
        return ET.parse(str(path)).getroot()
    except Exception as e:
        logger.warning(f"[BB] Could not parse {path.name}: {e}")
        return None


def _attr(elem, name: str, default: str = "") -> str:
    """Read an XML attribute safely."""
    if elem is None:
        return default
    return elem.get(name, default)


def _child_attr(elem, tag: str, attr: str, default: str = "") -> str:
    """Find a direct child by tag and return one of its attributes."""
    if elem is None:
        return default
    child = elem.find(tag)
    if child is None:
        return default
    return child.get(attr, default)


def _unescape_html(text: str) -> str:
    """Unescape HTML entities stored inside Blackboard TEXT elements."""
    if not text:
        return ""
    return html.unescape(text)


def _clean_bb_html(raw: str) -> str:
    """
    Unescape and lightly clean Blackboard HTML body content.

    Steps:
    1. Unescape HTML entities (BB stores body as escaped HTML inside XML TEXT nodes).
    2. Use BeautifulSoup to properly unwrap Blackboard Ultra layout wrapper divs
       (data-layout-row / data-layout-column / data-bbid) — regex fails on nested divs.
    3. Convert attachment-wrapper divs into clean, readable download links.
    4. Strip the @X@EmbeddedFile.requestUrlStub@X@ prefix from embedded file URLs.
    """
    if not raw:
        return ""

    unescaped = _unescape_html(raw)

    # Strip the EmbeddedFile stub prefix before parsing so URLs are clean
    unescaped = re.sub(r'@X@EmbeddedFile\.requestUrlStub@X@', '', unescaped)

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(unescaped, "html.parser")

        # ── Step 1: Convert attachment-wrapper divs into readable download links ──
        # Blackboard stores file attachments as:
        #   <div class="attachment-wrapper" data-filename="Foo.pdf" data-mimetype="application/pdf">
        #     <a class="bb-file-link" href="bbcswebdav/...">Attachment</a>
        #   </div>
        # We replace these with a clean paragraph + download link.
        for wrapper in soup.find_all("div", class_="attachment-wrapper"):
            filename = wrapper.get("data-filename", "")
            mimetype = wrapper.get("data-mimetype", "")
            # Find the existing <a> tag to get the href (may be a resolved CDN URL)
            a_tag = wrapper.find("a")
            href = a_tag.get("href", "") if a_tag else ""

            # Build a clean replacement
            if filename:
                ext = filename.rsplit(".", 1)[-1].upper() if "." in filename else ""
                label = f"📎 {filename}"
                if href and not href.startswith("bb"):
                    # Has a real URL — make a proper link
                    new_tag = soup.new_tag("p")
                    link = soup.new_tag("a", href=href, target="_blank", rel="noopener noreferrer")
                    link.string = label
                    new_tag.append(link)
                else:
                    # No resolved URL yet — show filename as plain text placeholder
                    new_tag = soup.new_tag("p")
                    new_tag.string = label
                wrapper.replace_with(new_tag)
            else:
                # No filename — just unwrap
                wrapper.unwrap()

        # ── Step 2: Unwrap Blackboard Ultra layout wrapper divs ───────────────────
        # These are purely structural and add no semantic value.
        # Must be done AFTER attachment-wrapper processing (they may be nested inside).
        for div in soup.find_all("div", attrs={"data-layout-column": True}):
            div.unwrap()
        for div in soup.find_all("div", attrs={"data-layout-row": True}):
            div.unwrap()
        # bbml editor wrapper
        for div in soup.find_all("div", attrs={"data-bbid": re.compile(r"bbml-editor-id")}):
            div.unwrap()

        # ── Step 3: Serialize back to HTML string ─────────────────────────────────
        result = str(soup)

        # Collapse excessive whitespace introduced by unwrapping
        result = re.sub(r'\n{3,}', '\n\n', result)
        result = re.sub(r'[ \t]{2,}', ' ', result)

        return result.strip()

    except ImportError:
        # BeautifulSoup not available — fall back to regex (best-effort)
        cleaned = re.sub(r'<div\s+data-layout-(?:row|column)[^>]*>', '', unescaped)
        cleaned = re.sub(r'<div\s+data-bbid="bbml-editor-id[^"]*"[^>]*>', '', cleaned)
        return cleaned.strip()


# ── Manifest parser ───────────────────────────────────────────────────────────

class _BBManifest:
    """
    Parses a Blackboard imsmanifest.xml.

    Builds:
      resources  : dict[identifier -> {bb_file, type, title}]
      toc_roots  : list of top-level <item> elements (the TOC trees)
    """

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.resources: Dict[str, Dict] = {}
        self.toc_roots: List = []
        self._root = None
        self._title_to_resource: Dict[str, Dict] = {}  # title → resource (for lookups)
        self._resource_redirects: Dict[str, Dict] = {}  # doc_res_id → richer resource (computed after parse)

    def parse(self) -> bool:
        import xml.etree.ElementTree as ET
        try:
            tree = ET.parse(str(self.manifest_path))
            self._root = tree.getroot()
        except Exception as e:
            logger.error(f"[BB] Failed to parse manifest: {e}")
            return False

        self._parse_resources()
        self._parse_organizations()
        return True

    def _parse_resources(self):
        """Build resource map from <resources> section."""
        resources_elem = self._root.find("resources")
        if resources_elem is None:
            return
        for res in resources_elem.findall("resource"):
            ident = res.get("identifier", "")
            bb_file = res.get("{http://www.blackboard.com/content-packaging/}file", "")
            res_type = res.get("type", "")
            title = res.get("{http://www.blackboard.com/content-packaging/}title", "")
            if ident:
                self.resources[ident] = {
                    "bb_file": bb_file,
                    "type": res_type,
                    "title": title,
                }
                # Index by title too (for cross-resolution)
                if title:
                    existing = self._title_to_resource.get(title)
                    new_type = res_type
                    # Prioritize richer resources over generic document wrappers
                    if existing:
                        existing_type = existing.get("type", "")
                        richer_types = ("assessment/x-bb-qti-test", "resource/x-bb-discussionboard", "resource/x-bb-announcement", "resource/x-bb-weblink")
                        generic_types = ("resource/x-bb-document", "course/x-bb-coursetoc")
                        if existing_type in richer_types and new_type in generic_types:
                            pass  # Keep the existing richer resource
                        else:
                            self._title_to_resource[title] = self.resources[ident]
                    else:
                        self._title_to_resource[title] = self.resources[ident]

        # After all resources loaded, build redirects:
        # Document wrappers that have a matching assessment/discussion resource
        # should redirect to the richer type.
        for rid, res in self.resources.items():
            res_type = res.get("type", "")
            title = res.get("title", "")
            # Document or course toc wrappers that have a matching non-doc resource
            if res_type in ("resource/x-bb-document", "course/x-bb-coursetoc"):
                if title and title in self._title_to_resource:
                    richer = self._title_to_resource[title]
                    richer_type = richer.get("type", "")
                    # Redirect if the match is a more specific type (assessment, discussion, announcement)
                    if richer_type in ("assessment/x-bb-qti-test", "resource/x-bb-discussionboard",
                                        "resource/x-bb-announcement", "resource/x-bb-weblink"):
                        # Use the richer resource's type and file
                        res["type"] = richer_type
                        res["bb_file"] = richer["bb_file"]
                        self._resource_redirects[rid] = richer
                        logger.info(f"[BB] Redirected {rid} ('{title}') → {richer_type} (file: {richer.get('bb_file')})")

    def _parse_organizations(self):
        """
        Collect top-level TOC item elements from the PRIMARY organization only.

        Blackboard exports contain multiple <organization> elements:
          - The first one (identifier="res00005" or similar) is the real course TOC.
          - "INTERACTIVE" holds discussion board shortcuts (duplicates).
          - "INDIRECT" holds indirect content references (duplicates).

        We only want the first/primary organization to avoid duplicate modules.
        """
        orgs = self._root.find("organizations")
        if orgs is None:
            return

        # Use the default organization if specified, otherwise take the first one
        default_id = orgs.get("default", "")
        all_orgs = orgs.findall("organization")
        if not all_orgs:
            return

        # Skip known Blackboard internal organization identifiers
        SKIP_ORG_IDS = {"INTERACTIVE", "INDIRECT"}

        primary = None
        for org in all_orgs:
            org_id = org.get("identifier", "")
            if org_id in SKIP_ORG_IDS:
                continue
            if default_id and org_id == default_id:
                primary = org
                break
            if primary is None:
                primary = org  # take first non-skipped org

        if primary is None:
            return

        for item in primary.findall("item"):
            self.toc_roots.append(item)


# ── .dat file readers ─────────────────────────────────────────────────────────

class _BBContentReader:
    """Reads a Blackboard <CONTENT> .dat file."""

    def __init__(self, path: Path):
        self.path = path
        self._root = _parse_xml(path)

    @property
    def title(self) -> str:
        if self._root is None:
            return self.path.stem
        return self._root.get("id", "") and _child_attr(
            self._root, "TITLE", "value", self.path.stem
        )

    @property
    def is_folder(self) -> bool:
        """True if this item is a container (folder/page), not a leaf content item."""
        if self._root is None:
            return False
        handler = _child_attr(self._root, "CONTENTHANDLER", "value", "")
        return "folder" in handler.lower()

    @property
    def body_html(self) -> str:
        """Return the HTML body, unescaped and cleaned."""
        if self._root is None:
            return ""
        body = self._root.find("BODY")
        if body is None:
            return ""
        text_elem = body.find("TEXT")
        if text_elem is None or not text_elem.text:
            return ""
        body_type = _child_attr(body, "TYPE", "value", "")
        if body_type == "H":
            # HTML content stored as escaped string
            return _clean_bb_html(text_elem.text)
        elif body_type == "S":
            # Plain text / empty
            return text_elem.text.strip() if text_elem.text else ""
        return _clean_bb_html(text_elem.text) if text_elem.text else ""

    @property
    def description(self) -> str:
        """Return the plain-text DESCRIPTION attribute (used for assignment instructions)."""
        if self._root is None:
            return ""
        desc_elem = self._root.find("DESCRIPTION")
        if desc_elem is None:
            return ""
        return desc_elem.get("value", "").strip()

    @property
    def is_available(self) -> bool:
        flags = self._root.find("FLAGS") if self._root is not None else None
        if flags is None:
            return True
        avail = flags.find("ISAVAILABLE")
        if avail is None:
            return True
        return avail.get("value", "true").lower() == "true"


class _BBDiscussionReader:
    """Reads a Blackboard <FORUM> .dat file."""

    def __init__(self, path: Path):
        self.path = path
        self._root = _parse_xml(path)

    @property
    def title(self) -> str:
        if self._root is None:
            return self.path.stem
        return _child_attr(self._root, "TITLE", "value", self.path.stem)

    @property
    def body_html(self) -> str:
        """Return the discussion prompt HTML."""
        if self._root is None:
            return ""
        # Try description first
        desc = self._root.find("DESCRIPTION")
        if desc is not None:
            text = desc.find("TEXT")
            if text is not None and text.text:
                return _clean_bb_html(text.text)
        # Fall back to first message thread
        threads = self._root.find("MESSAGETHREADS")
        if threads is not None:
            msg = threads.find("MSG")
            if msg is not None:
                mt = msg.find("MESSAGETEXT")
                if mt is not None:
                    text = mt.find("TEXT")
                    if text is not None and text.text:
                        return _clean_bb_html(text.text)
        return ""


class _BBAssessmentReader:
    """
    Reads a Blackboard QTI assessment .dat file.
    Extracts title, max score, and questions.
    """

    def __init__(self, path: Path):
        self.path = path
        self._root = _parse_xml(path)

    @property
    def title(self) -> str:
        if self._root is None:
            return self.path.stem
        assessment = self._root.find("assessment")
        if assessment is None:
            return self.path.stem
        return assessment.get("title", self.path.stem)

    @property
    def max_score(self) -> float:
        if self._root is None:
            return 0.0
        meta = self._root.find(".//assessmentmetadata")
        if meta is None:
            return 0.0
        score_elem = meta.find("qmd_absolutescore_max")
        if score_elem is not None and score_elem.text:
            try:
                return float(score_elem.text)
            except ValueError:
                pass
        return 0.0

    @property
    def is_assignment(self) -> bool:
        """
        True when the QTI file is actually an assignment submission portal.
        Blackboard exports assignment drop-boxes as QTI assessments with
        bbmd_assessment_subtype = 'Assignment'.
        """
        if self._root is None:
            return False
        meta = self._root.find(".//assessmentmetadata")
        if meta is None:
            return False
        subtype = meta.find("bbmd_assessment_subtype")
        return subtype is not None and (subtype.text or "").strip().lower() == "assignment"

    def get_questions(self) -> List[Dict]:
        """Extract question text and answer choices."""
        questions = []
        if self._root is None:
            return questions

        for item in self._root.findall(".//item"):
            meta = item.find("itemmetadata")
            q_type = ""
            if meta is not None:
                qt = meta.find("bbmd_questiontype")
                q_type = qt.text if qt is not None and qt.text else ""

            # Question text
            q_text = ""
            for fmt in item.findall(".//mat_formattedtext"):
                if fmt.text:
                    q_text = _clean_bb_html(fmt.text)
                    break

            # Answer choices
            choices = []
            for label in item.findall(".//response_label"):
                for fmt in label.findall(".//mat_formattedtext"):
                    if fmt.text:
                        choices.append(_clean_bb_html(fmt.text))
                        break
                else:
                    # Plain text answer
                    for mt in label.findall(".//mattext"):
                        if mt.text:
                            choices.append(mt.text.strip())
                        break

            if q_text:
                questions.append({
                    "text": q_text,
                    "type": q_type,
                    "choices": choices,
                })

        return questions


# ── Course settings reader ────────────────────────────────────────────────────

def _read_course_title(course_dir: Path, resources: Dict[str, Dict]) -> Tuple[str, str]:
    """
    Read the real course title and course ID from the course-settings .dat file.
    Returns (title, course_id).
    """
    for res_id, res in resources.items():
        if res.get("type", "") == "course/x-bb-coursesetting":
            dat_path = course_dir / res["bb_file"]
            if dat_path.exists():
                root = _parse_xml(dat_path)
                if root is not None:
                    title_elem = root.find("TITLE")
                    courseid_elem = root.find("COURSEID")
                    title = title_elem.get("value", "") if title_elem is not None else ""
                    course_id = courseid_elem.get("value", "") if courseid_elem is not None else ""
                    if title:
                        return title, course_id
    return "Untitled Course", ""


# ── TOC walker ────────────────────────────────────────────────────────────────

def _walk_toc(
    item_elem,
    resources: Dict[str, Dict],
    course_dir: Path,
    depth: int = 0,
) -> Optional[Dict]:
    """
    Recursively walk a TOC <item> element and return a structured dict:
      {title, identifier, type, body, children, bb_type}

    Blackboard Ultra structure:
      <item identifierref="res00040">   ← WEEK 1 folder
        <item identifierref="res00050"> ← Learning Outcomes folder
          <item identifierref="res00081"> ← ultraDocumentBody (real HTML)
        <item identifierref="res00065"> ← WK 1 Instructions folder
          <item identifierref="res00089"> ← ultraDocumentBody (real HTML)
        <item identifierref="res00072"> ← Orientation Quiz (assessment)
    """
    ident_ref = item_elem.get("identifierref", "")
    title_elem = item_elem.find("title")
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

    res = resources.get(ident_ref, {})
    bb_type = res.get("type", "")
    bb_file = res.get("bb_file", "")

    node = {
        "title": title or res.get("title", ident_ref),
        "identifier": ident_ref,
        "bb_type": bb_type,
        "bb_file": bb_file,
        "body": "",
        "children": [],
        "is_folder": False,
        "is_ultra_body": title == "ultraDocumentBody",
    }

    # Read the .dat file if it exists
    if bb_file:
        dat_path = course_dir / bb_file
        if dat_path.exists():
            if "discussionboard" in bb_type:
                reader = _BBDiscussionReader(dat_path)
                node["body"] = reader.body_html
                node["bb_type"] = "discussion"
            elif "qti-test" in bb_type or "assessment" in bb_type.lower():
                reader = _BBAssessmentReader(dat_path)
                if reader.is_assignment:
                    # This is an assignment submission portal, not a quiz.
                    # Try to read the original CONTENT .dat for the description/instructions.
                    # The manifest redirect changed bb_file to the QTI file, but the
                    # original CONTENT .dat has the same identifier (e.g. res00036.dat).
                    node["bb_type"] = "assignment"
                    node["max_score"] = reader.max_score
                    node["body"] = f"<p><strong>{reader.title}</strong></p>"
                    # Read the original CONTENT .dat (same name as identifier) for description
                    original_dat = course_dir / f"{node['identifier']}.dat"
                    if original_dat.exists():
                        content_reader = _BBContentReader(original_dat)
                        desc = content_reader.description
                        if desc:
                            node["description"] = desc
                else:
                    node["body"] = f"<p><strong>{reader.title}</strong></p>"
                    node["max_score"] = reader.max_score
                    node["questions"] = reader.get_questions()
                    node["bb_type"] = "assessment"
            elif "document" in bb_type or "coursetoc" in bb_type or "lesson" in bb_type:
                reader = _BBContentReader(dat_path)
                node["is_folder"] = reader.is_folder
                node["body"] = reader.body_html
                # Capture DESCRIPTION separately — used as fallback in _node_to_items
                # ONLY when body is still empty after ultraDocumentBody merge.
                # Do NOT set node["body"] here from description — that would block
                # the ultraDocumentBody merge in _node_to_items.
                desc = reader.description
                if desc:
                    node["description"] = desc
                # Detect LTI/external tool handlers — mark as external_tool type
                handler_val = ""
                try:
                    import xml.etree.ElementTree as _ET
                    _root = _ET.parse(str(dat_path)).getroot()
                    _h = _root.find("CONTENTHANDLER")
                    if _h is not None:
                        handler_val = _h.get("value", "")
                except Exception:
                    pass
                if "lti" in handler_val.lower() or "bltiplacement" in handler_val.lower():
                    node["bb_type"] = "external_tool"
                    node["is_folder"] = False
            elif "link" in bb_type:
                node["bb_type"] = "link"

    # Recurse into children
    for child_elem in item_elem.findall("item"):
        child = _walk_toc(child_elem, resources, course_dir, depth + 1)
        if child:
            node["children"].append(child)

    return node


# ── Node → CanvasModuleItem converter ────────────────────────────────────────

def _node_to_items(node: Dict) -> List[CanvasModuleItem]:
    """
    Convert a TOC node (and its children) into a flat list of CanvasModuleItem.

    Rules:
    - Skip nodes titled "ultraDocumentBody" — their content is merged into parent.
    - Folders with no body and no meaningful children are skipped.
    - Assessments become Quiz items.
    - Discussions become Discussion items.
    - Everything else becomes a page/lesson.
    - The ultraDocumentBody child's HTML is merged into the parent folder's body.
    """
    items = []

    bb_type = node.get("bb_type", "")
    title = node.get("title", "")
    body = node.get("body", "")
    children = node.get("children", [])

    # Skip internal Blackboard structural nodes
    if title in ("--TOP--",):
        # Recurse into children directly
        for child in children:
            items.extend(_node_to_items(child))
        return items

    # Merge ultraDocumentBody children into this node's body
    real_children = []
    for child in children:
        if child.get("is_ultra_body") or child.get("title") == "ultraDocumentBody":
            if child.get("body") and not body:
                body = child.get("body", "")
            # Don't add ultraDocumentBody as a separate item
        else:
            real_children.append(child)

    # If still no body after ultraDocumentBody merge, fall back to description.
    # Only for non-folder nodes — folder descriptions are structural labels, not content.
    if not body and not node.get("is_folder", False):
        desc = node.get("description", "")
        if desc:
            import html as _html_mod
            body = "<p>" + _html_mod.escape(desc).replace("\n", "</p><p>") + "</p>"

    # Determine content type based on Blackboard resource type
    # Blackboard types are namespaced, e.g.:
    #   "assessment/x-bb-qti-test" → quiz
    #   "resource/x-bb-discussionboard" → discussion
    #   "resource/x-bb-weblink" → weblink
    #   "resource/x-bb-announcement" → page (announcement body)
    #   "course/x-bb-coursetoc" → folder → page
    #   "resource/x-bb-document" → page
    if "assessment" in bb_type or "qti-test" in bb_type:
        content_type = "quiz"
    elif bb_type == "assignment":
        content_type = "assignment"
    elif "discussion" in bb_type or "forum" in bb_type:
        content_type = "discussion"
    elif "link" in bb_type or "weblink" in bb_type:
        content_type = "weblink"
    elif bb_type == "external_tool":
        content_type = "external_tool"
    elif "assignment" in bb_type:
        content_type = "assignment"
    else:
        content_type = "page"

    # Build the item
    item = CanvasModuleItem(
        title=title,
        identifier=node.get("identifier", ""),
        content_type=content_type,
        content_file=None,
        position=0,
        workflow_state=WorkflowState.ACTIVE,
        items=[],
    )
    # Attach body and extra data as custom attributes
    item._bb_body = body
    item._bb_max_score = node.get("max_score", 0.0)
    item._bb_questions = node.get("questions", [])
    item._bb_description = node.get("description", "")  # assignment instructions
    item._content_ref = node.get("identifier", "")

    # Only add the item if it has content OR is a meaningful typed item.
    # Skip structural containers that have no renderable content after all merges.
    # A folder that received content from its ultraDocumentBody child IS renderable.
    has_content = bool(body and body.strip())
    is_typed = content_type in ("quiz", "discussion", "assignment", "weblink", "external_tool")
    # A node is a pure structural container only if it's a folder AND has no
    # content after ultraDocumentBody merge AND is not a typed item.
    is_empty_folder = node.get("is_folder", False) and not has_content and not is_typed

    if not is_empty_folder and (has_content or is_typed):
        items.append(item)

    # Recurse into real children (sub-items within a folder)
    for child in real_children:
        items.extend(_node_to_items(child))

    return items


# ── Main adapter ──────────────────────────────────────────────────────────────

class BlackboardAdapter:
    """
    Adapter for Blackboard Learn Ultra export packages.

    Accepts either:
      - A .zip / .imscc file path
      - An already-extracted directory

    Returns a CanvasCourse object compatible with the existing pipeline.
    """

    def load(self, payload: Dict[str, Any]) -> CanvasCourse:
        zip_path = Path(payload["zip_path"])

        # Extract if it's a zip/imscc file
        extract_dir = None
        cleanup = False
        if zip_path.is_file():
            extract_dir = Path(tempfile.mkdtemp(prefix="bb_extract_"))
            cleanup = True
            logger.info(f"[BB] Extracting {zip_path.name} → {extract_dir}")
            with zipfile.ZipFile(zip_path, "r") as zf:
                safe_extractall(zf, extract_dir)
            # Traverse into single top-level dir if present
            items = list(extract_dir.iterdir())
            if len(items) == 1 and items[0].is_dir():
                extract_dir = items[0]
        else:
            extract_dir = zip_path

        try:
            course = self._parse(extract_dir, payload)
            course.source_directory = str(extract_dir)
            # Keep extract_dir alive — asset uploader may need it
            # Cleanup is handled by IngestionWorker after the full pipeline
            return course
        except Exception:
            if cleanup and extract_dir and extract_dir.exists():
                shutil.rmtree(extract_dir, ignore_errors=True)
            raise

    def _parse(self, course_dir: Path, payload: Dict[str, Any]) -> CanvasCourse:
        manifest_path = course_dir / "imsmanifest.xml"
        if not manifest_path.exists():
            raise ValueError(f"imsmanifest.xml not found in {course_dir}")

        # ── 1. Parse manifest ─────────────────────────────────────────────────
        manifest = _BBManifest(manifest_path)
        if not manifest.parse():
            raise ValueError("Failed to parse Blackboard manifest")

        resources = manifest.resources

        # ── 2. Read real course title from course-settings .dat ───────────────
        course_title, course_id_bb = _read_course_title(course_dir, resources)
        logger.info(f"[BB] Course: {course_title}  ({course_id_bb})")

        # ── 3. Walk TOC trees to build module/item structure ──────────────────
        modules: List[CanvasModule] = []
        pages: List[CanvasPage] = []
        quizzes: List[CanvasQuiz] = []
        assignments: List[CanvasAssignment] = []
        discussions: List[CanvasDiscussion] = []

        for toc_root in manifest.toc_roots:
            toc_title_elem = toc_root.find("title")
            toc_title = (
                toc_title_elem.text.strip()
                if toc_title_elem is not None and toc_title_elem.text
                else "Untitled"
            )

            # Skip Blackboard internal TOC containers (INTERACTIVE, INDIRECT)
            # that are not real content modules
            if toc_title in ("INTERACTIVE", "INDIRECT"):
                logger.debug(f"[BB] Skipping internal TOC: {toc_title}")
                continue

            # Walk the TOC tree
            toc_node = _walk_toc(toc_root, resources, course_dir)
            if not toc_node:
                continue

            # The ROOT TOC wraps everything in a single "--TOP--" child.
            # Unwrap it: treat --TOP--'s children as the real module list.
            raw_children = toc_node.get("children", [])
            if (
                len(raw_children) == 1
                and raw_children[0].get("title") == "--TOP--"
            ):
                module_nodes = raw_children[0].get("children", [])
            else:
                module_nodes = raw_children

            for mod_node in module_nodes:
                mod_title = mod_node.get("title", "Untitled Module")

                # Skip --TOP-- structural nodes at module level
                if mod_title == "--TOP--":
                    continue

                # Build items for this module
                mod_items = _node_to_items(mod_node)

                # Convert items to CanvasModuleItem and collect content objects
                canvas_items = []
                for item in mod_items:
                    canvas_items.append(item)

                    body = getattr(item, "_bb_body", "")
                    description = getattr(item, "_bb_description", "")
                    item_id = item.identifier

                    if item.content_type == "quiz":
                        from models.canvas_models import CanvasQuiz
                        q = CanvasQuiz(
                            title=item.title,
                            identifier=item_id,
                            description=body,
                            points_possible=getattr(item, "_bb_max_score", 0.0),
                            questions=[],
                        )
                        quizzes.append(q)

                    elif item.content_type == "assignment":
                        from models.canvas_models import CanvasAssignment
                        # Use DESCRIPTION from the CONTENT wrapper as instructions.
                        # Fall back to body if description is empty.
                        instructions = description or body or f"<p>{item.title}</p>"
                        a = CanvasAssignment(
                            title=item.title,
                            identifier=item_id,
                            description=instructions,
                            points_possible=getattr(item, "_bb_max_score", 0.0),
                        )
                        assignments.append(a)

                    elif item.content_type == "discussion":
                        from models.canvas_models import CanvasDiscussion
                        d = CanvasDiscussion(
                            title=item.title,
                            identifier=item_id,
                            body=body,
                        )
                        discussions.append(d)

                    else:
                        # Lesson / page
                        page = CanvasPage(
                            title=item.title,
                            identifier=item_id,
                            body=body,
                            workflow_state=WorkflowState.ACTIVE,
                            source_file=str(course_dir / resources.get(item_id, {}).get("bb_file", "")),
                        )
                        pages.append(page)

                if canvas_items:
                    module = CanvasModule(
                        title=mod_title,
                        identifier=mod_node.get("identifier", ""),
                        position=len(modules),
                        items=canvas_items,
                        workflow_state=WorkflowState.ACTIVE,
                    )
                    modules.append(module)

        # ── 4. Build CanvasCourse ─────────────────────────────────────────────
        bb_resources = {}
        for res_id, res in resources.items():
            bb_file = res.get("bb_file") or ""
            res_type = res.get("type") or ""
            title = res.get("title") or None
            bb_resources[res_id] = CanvasResource(
                identifier=res_id,
                href=bb_file or None,
                type=res_type,
                title=title,
            )

        course = CanvasCourse(
            title=course_title,
            identifier=course_id_bb or manifest_path.parent.name,
            modules=modules,
            resources=bb_resources,
            pages=pages,
            quizzes=quizzes,
            assignments=assignments,
            discussions=discussions,
            weblinks=[],
            source_directory=str(course_dir),
        )
        # Attach the Blackboard course ID as a secondary identifier so the
        # IngestionWorker can extract the course code (e.g. MGMT-5306) from it.
        course._bb_course_id = course_id_bb

        logger.info(
            f"[BB] Parsed: {len(modules)} modules, {len(pages)} pages, "
            f"{len(quizzes)} quizzes, {len(discussions)} discussions"
        )
        return course
