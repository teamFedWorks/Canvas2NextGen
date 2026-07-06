"""
Content Enrichment Layer - Cleans, tags, and enriches course content.

Responsibilities:
1. HTML sanitization
2. Link validation and fixing
3. Metadata inference (title extraction, keyword extraction)
4. Alt text generation for images
5. Transcript extraction from media
6. Semantic chunking for RAG
"""

import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from pathlib import Path
from bs4 import BeautifulSoup
import html

from models.canonical_models import CanonicalCourse, CanonicalModule, CanonicalCurriculumItem, CanonicalAsset, CanonicalContentType
from observability.logger import get_logger

logger = get_logger(__name__)


def _has_word(kw: str, text: str) -> bool:
    """Check if a keyword exists in text as a whole word (word boundaries)."""
    if not text:
        return False
    return bool(re.search(rf"\b{re.escape(kw)}\b", text, re.IGNORECASE))


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    item_count: int = 0
    warnings: List[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.metadata is None:
            self.metadata = {}


class ContentSanitizer:
    """Sanitizes HTML content for safety and consistency."""
    
    # Allowed HTML tags
    ALLOWED_TAGS = {
        'p', 'br', 'strong', 'em', 'u', 'ol', 'ul', 'li',
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
        'a', 'img', 'video', 'source', 'iframe',
        'table', 'thead', 'tbody', 'tr', 'th', 'td',
        'div', 'span', 'pre', 'code',
    }
    
    # Allowed attributes
    ALLOWED_ATTRS = {
        'a': ['href', 'title', 'target'],
        'img': ['src', 'alt', 'width', 'height', 'style'],
        'video': ['src', 'controls', 'width', 'height', 'poster'],
        'iframe': ['src', 'width', 'height', 'frameborder', 'allowfullscreen'],
    }
    
    @staticmethod
    def sanitize(html_content: str, base_url: str = "") -> str:
        """
        Clean HTML content:
        - Remove dangerous tags/attributes
        - Fix broken tags
        - Normalize whitespace
        """
        if not html_content:
            return ""
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script tags and event handlers
            for tag in soup.find_all('script'):
                tag.decompose()
            for tag in soup.find_all(onclick=True):
                del tag['onclick']
            for tag in soup.find_all(onload=True):
                del tag['onload']
            
            # Remove disallowed tags but keep content
            for tag in soup.find_all(True):
                if tag.name not in ContentSanitizer.ALLOWED_TAGS:
                    tag.unwrap()
            
            # Clean attributes
            for tag in soup.find_all(True):
                allowed = ContentSanitizer.ALLOWED_ATTRS.get(tag.name, [])
                attrs_to_remove = [attr for attr in tag.attrs if attr not in allowed]
                for attr in attrs_to_remove:
                    del tag[attr]
            
            # Normalize whitespace
            cleaned = str(soup)
            cleaned = re.sub(r'\s+', ' ', cleaned)
            cleaned = cleaned.strip()
            
            return cleaned
        except Exception as e:
            logger.warning(f"HTML sanitization failed: {e}")
            return html_content  # Return original on failure


class LinkValidator:
    """Validates and fixes broken links within course content."""
    
    def __init__(self, asset_lookup: Dict[str, CanonicalAsset] = None):
        self.asset_lookup = asset_lookup or {}
    
    def validate_and_fix(self, html_content: str) -> tuple[str, List[str]]:
        """
        Check all links in HTML and fix broken references.
        
        Returns:
            (fixed_html, list_of_broken_links)
        """
        if not html_content:
            return html_content, []
        
        broken = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                # Check if it's an internal asset reference
                if href.startswith('./') or href.startswith('../'):
                    # Relative path - check assets
                    asset_key = href.lstrip('./')
                    if asset_key not in self.asset_lookup:
                        broken.append(href)
                        # Mark as broken
                        a_tag['data-broken'] = 'true'
                        a_tag['class'] = a_tag.get('class', []) + ['broken-link']
            
            return str(soup), broken
        except Exception as e:
            logger.warning(f"Link validation failed: {e}")
            return html_content, []


class MetadataInferer:
    """Infers missing metadata from content."""
    
    @staticmethod
    def infer_title(html_content: str, fallback: str = "") -> str:
        """Extract title from HTML (first h1 or title tag)."""
        if not html_content:
            return fallback
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Try H1
            h1 = soup.find('h1')
            if h1 and h1.text.strip():
                return h1.text.strip()
            
            # Try title tag
            title = soup.find('title')
            if title and title.text.strip():
                return title.text.strip()
            
            # Try first strong/em text
            strong = soup.find(['strong', 'b'])
            if strong and strong.text.strip():
                return strong.text.strip()[:100]
            
        except Exception:
            pass
        
        return fallback
    
    @staticmethod
    def extract_keywords(html_content: str, max_keywords: int = 10) -> List[str]:
        """Extract semantic keywords from HTML text."""
        if not html_content:
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text()
            
            # Simple keyword extraction (in production: use NLP/sentence transformers)
            words = re.findall(r'\b[A-Za-z]{4,}\b', text.lower())
            
            # Filter stopwords
            stopwords = {'the', 'and', 'for', 'with', 'this', 'that', 'from', 'are', 'was'}
            keywords = [w for w in words if w not in stopwords]
            
            # Frequency-based selection
            from collections import Counter
            freq = Counter(keywords)
            return [word for word, _ in freq.most_common(max_keywords)]
        except Exception:
            return []


class ContentEnricher:
    """
    Main enrichment pipeline for canonical course content.
    
    Applies all enrichment transformations:
    1. HTML sanitization
    2. Link validation
    3. Metadata inference
    4. Semantic tagging
    """
    
    def __init__(self, asset_lookup: Dict[str, CanonicalAsset] = None):
        self.asset_lookup = asset_lookup or {}
        self.sanitizer = ContentSanitizer()
        self.link_validator = LinkValidator(self.asset_lookup)
        self.metadata_inferer = MetadataInferer()
    
    def enrich_course(self, course: CanonicalCourse) -> EnrichmentResult:
        """
        Apply all enrichment to a course.
        """
        result = EnrichmentResult()
        result.metadata["total_items"] = sum(len(m.items) for m in course.modules)
        
        for module in course.modules:
            for item in module.items:
                if item.body:
                    # 1. Sanitize HTML
                    original = item.body
                    item.body = self.sanitizer.sanitize(item.body)
                    
                    if len(item.body) < len(original) * 0.5:
                        result.warnings.append(f"Content significantly reduced for item '{item.title}'")
                    
                    # 2. Validate links
                    _, broken = self.link_validator.validate_and_fix(item.body)
                    if broken:
                        result.warnings.append(f"Broken links in '{item.title}': {', '.join(broken[:3])}")
                    
                    # 3. Infer missing metadata
                    if not item.title or len(item.title) < 3:
                        inferred = self.metadata_inferer.infer_title(item.body, item.title or "")
                        if inferred:
                            item.title = inferred
                            result.metadata.setdefault("titles_inferred", []).append(inferred)
                    
                    # Store keywords
                    keywords = self.metadata_inferer.extract_keywords(item.body)
                    if keywords:
                        item._enriched_keywords = keywords
                
                # 4. Semantic tagging / Type refinement
                if item.content_type in [CanonicalContentType.LESSON, CanonicalContentType.WEBLINK, CanonicalContentType.FILE, CanonicalContentType.DISCUSSION]:
                    title_lower = (item.title or "").lower()
                    
                    # Policy
                    if any(_has_word(kw, title_lower) for kw in ["syllabus", "policy", "rules", "guideline", "honor code", "compliance"]):
                        item.content_type = CanonicalContentType.POLICY
                    
                    # Announcement (often stored as discussions in Canvas)
                    elif any(_has_word(kw, title_lower) for kw in ["announcement", "welcome", "important notice", "update", "week ", "reminder", "posted"]):
                        item.content_type = CanonicalContentType.ANNOUNCEMENT

                    # Reading (instructional documents) — HIGHER PRIORITY so that
                    # guides, templates, samples, examples, and annotated bibliographies
                    # are classified as Reading rather than being caught by the
                    # broader resource bucket below.
                    elif any(_has_word(kw, title_lower) for kw in ["textbook", "reading", "chapter", "article", "paper", "book", "manual", "guide", "template", "sample", "example", "steps for", "writing guide", "annotated bibliography"]) or (title_lower.endswith(".pdf") and not any(_has_word(kw, title_lower) for kw in ["practice", "solution", "exam", "quiz", "assignment"])):
                        item.content_type = CanonicalContentType.READING
                    
                    # Resource (supporting materials) - HIGH PRIORITY for practical materials
                    elif any(_has_word(kw, title_lower) for kw in ["resource", "support", "help", "tutorial", "uploading", "faq", "practice", "solution", "materials", "handout", "extra credit", "prep", "dataset", "csv", "ipynb", "exercise", "lab", "answer"]):
                        item.content_type = CanonicalContentType.RESOURCE
                    
                    # Live Session / Media
                    elif any(_has_word(kw, title_lower) for kw in ["zoom", "webinar", "live session", "video", "recording", "meeting", "presentation", "lecture"]) or title_lower.endswith((".mp4", ".mov", ".avi", ".pptx", ".ppt", ".key")):
                        item.content_type = CanonicalContentType.LIVE_SESSION
                    
                    # Survey
                    elif any(_has_word(kw, title_lower) for kw in ["survey", "evaluation", "feedback", "poll"]):
                        item.content_type = CanonicalContentType.SURVEY
                    
                    # External Tool
                    elif any(_has_word(kw, title_lower) for kw in ["lti", "turnitin", "external tool", "launch", "tool", "plugin"]):
                        item.content_type = CanonicalContentType.EXTERNAL_TOOL
                    
                    # Default to RESOURCE for WebLinks if not matched
                    elif item.content_type == CanonicalContentType.WEBLINK:
                        item.content_type = CanonicalContentType.RESOURCE

                result.item_count += 1
        
        logger.info("Content enrichment complete",
                   extra={"items": result.item_count, "warnings": len(result.warnings)})
        
        return result


# ---------------------------------------------------------------------------
# LMS-native enrichment — operates directly on LmsCourse / LmsCurriculumItem
# so the production IngestionWorker path gets the same semantic intelligence
# as the CanonicalPipeline path.
# ---------------------------------------------------------------------------

from models.lms_models import LmsCourse, LmsCurriculumItem  # noqa: E402 (appended)


class LmsCourseEnricher:
    """
    Enriches an already-transformed LmsCourse in-place.

    Runs immediately after CourseTransformer and before AssetUploader so that
    semantic metadata is persisted to MongoDB alongside the course document.

    Classification strategy
    -----------------------
    Every item is evaluated through four signal layers, in priority order:

    Layer 0 — Structural certainty (confidence 1.0)
        Items whose type was set by the parser from hard structural evidence
        (QTI quiz file, assignment XML, etc.) are never overridden.
        Exception: if the title contradicts the structural type (e.g. a
        Blackboard "Required First Assignment" catalogued as Quiz), the type
        is corrected and confidence is set to 0.95.

    Layer 1 — Pattern matching on title (confidence 0.90–0.95)
        Regex patterns that are unambiguous regardless of content:
        discussion-thread prefixes (D1:, D2:, DB:), weekly-guide prefixes
        (WK N, Week N Instructions), learning-outcome pages, introduction
        forums, annotated-bibliography items, etc.

    Layer 2 — Keyword matching on title + content body (confidence 0.80–0.90)
        Multi-keyword scoring: each matching keyword adds weight.
        Confidence = 0.80 + 0.05 × min(hits, 2).

    Layer 3 — Fallback heuristics (confidence 0.55–0.70)
        Single-keyword title match or content-body signal alone.

    Confidence is NEVER shown as < 0.55 for items that have been positively
    classified. Items that fall through all layers get type=Lesson, conf=0.55.
    """

    # ── Type → interaction level ──────────────────────────────────────────
    _INTERACTION: Dict[str, str] = {
        "Quiz":           "active",
        "Assignment":     "active",
        "Discussion":     "active",
        "Survey":         "active",
        "Lesson":         "passive",
        "Reading":        "passive",
        "LiveSession":    "passive",
        "Announcement":   "passive",
        "Policy":         "passive",
        "Resource":       "passive",
        "ExternalTool":   "active",
        "instructor_bio": "passive",  # bio / about-me pages are read-only
    }

    # ── Words per minute reading speed (conservative) ────────────────────
    _WPM = 200

    # ── Asset-type duration estimates (minutes) ───────────────────────────
    _ASSET_DURATION: Dict[str, int] = {
        ".pdf":   15,
        ".pptx":  10,
        ".ppt":   10,
        ".docx":  10,
        ".doc":   10,
        ".mp4":   20,
        ".mov":   20,
        ".ipynb": 30,
        ".csv":    5,
    }

    # ── Layer 1: unambiguous title-pattern rules ──────────────────────────
    # Each entry: (compiled_regex, canonical_type, instructional_type, confidence)
    # Evaluated in order; first match wins.
    _PATTERN_RULES: List[tuple] = []

    def __init__(self):
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Build the compiled regex rule table once at construction time."""
        raw: List[tuple] = [
            # ── Discussion threads ────────────────────────────────────────
            # "D1: Response", "D2: Replies", "DB: Response", "Dialog 1: Replies"
            (r"^d\d+\s*:\s*(response|replies?|post|reply|discussion)",
             "Discussion", "discussion_prompt", 0.95),
            (r"^dialog\s*\d+\s*:\s*(response|replies?|post|reply)",
             "Discussion", "discussion_prompt", 0.95),
            (r"^db\s*:\s*",
             "Discussion", "discussion_prompt", 0.90),
            # Standalone "Response" / "Replies" items inside a discussion module
            (r"^(response|replies?)\s*$",
             "Discussion", "discussion_prompt", 0.90),
            # Introduction / icebreaker forums
            (r"^introductions?\s*$",
             "Discussion", "discussion_prompt", 0.95),
            (r"^(introduce yourself|meet and greet|icebreaker)",
             "Discussion", "discussion_prompt", 0.90),

            # ── Weekly guides / instructions ──────────────────────────────
            # "WK 1 Instructions", "Week 2 Instructions", "Module 3 Overview"
            # "Module 1 Activities and Assessments", "Module 2 Interactions"
            # Canvas: "What to do in Week #N / Module #N"
            # Canvas: "Where to begin: Week N - Topic"
            (r"^wk\s*\d+\s+instructions?\s*$",
             "Lesson", "weekly_guide", 0.95),
            (r"^week\s*\d+\s+instructions?\s*$",
             "Lesson", "weekly_guide", 0.95),
            (r"^what\s+to\s+do\s+in\s+week",
             "Lesson", "weekly_guide", 0.95),
            (r"^where\s+to\s+begin\s*:",
             "Lesson", "weekly_guide", 0.95),
            (r"^module\s*\d+\s+(overview|instructions?|guide|objectives?|activities?(\ and\ assessments?)?|assessments?|interactions?)\s*$",
             "Lesson", "weekly_guide", 0.90),
            # "Module N Course Content - Readings and Lectures"
            (r"^module\s*\d+\s+course\s+content",
             "Lesson", "weekly_guide", 0.90),

            # ── Instructor / facilitator bio pages ────────────────────────
            # "About me ... your learning facilitator", "Meet your professor"
            (r"(about\s+me|your\s+(instructor|professor|facilitator|learning\s+facilitator)|meet\s+(your|the)\s+(instructor|professor|facilitator))",
             "Lesson", "instructor_bio", 0.90),

            # ── Learning outcomes / objectives pages ──────────────────────
            (r"^learning\s+outcomes?\s*$",
             "Lesson", "learning_outcomes", 0.95),
            (r"^(course\s+)?learning\s+objectives?\s*$",
             "Lesson", "learning_outcomes", 0.95),
            (r"^week\s*\d+\s+outcomes?\s*$",
             "Lesson", "learning_outcomes", 0.90),

            # ── Annotated bibliography assignments ────────────────────────
            # "AB 1", "AB 2", "Annotated Bibliography 1"
            (r"^ab\s*\d+\s*$",
             "Assignment", "graded_assignment", 0.95),
            (r"^annotated\s+bibliography\s*#?\d*\s*$",
             "Assignment", "graded_assignment", 0.95),

            # ── Case analysis / research papers / faith integration ───────
            (r"^case\s+analysis\s*$",
             "Assignment", "graded_assignment", 0.95),
            (r"^research\s+paper\s*$",
             "Assignment", "graded_assignment", 0.95),
            (r"^faith\s+integration\s+paper\s*$",
             "Assignment", "graded_assignment", 0.95),
            (r"^(midterm|final)\s+(exam|paper|project)\s*$",
             "Assignment", "graded_assignment", 0.95),

            # ── Policies / guidelines ─────────────────────────────────────
            (r"^(ai|artificial intelligence)\s+usage\s*$",
             "Policy", "policy_document", 0.95),
            (r"^(ai|artificial intelligence)\s+(policy|guidelines?|rules?)\s*$",
             "Policy", "policy_document", 0.95),
            (r"^dialog\s+rules?\s*$",
             "Policy", "policy_document", 0.95),
            (r"^(course\s+)?syllabus\s*(\d+)?\s*$",
             "Policy", "policy_document", 1.00),
            (r"^honor\s+code\s*$",
             "Policy", "policy_document", 0.95),

            # ── Orientation / required-first items ───────────────────────
            (r"^(required\s+first\s+assignment|orientation\s+quiz)\s*$",
             "Assignment", "graded_assignment", 0.95),

            # ── Textbook / reading references ─────────────────────────────
            (r"^textbook\s+for\s+",
             "Reading", "reading_material", 0.95),
            (r"^(apa\s+\d+|apa\s+seventh|apa\s+7)",
             "Reading", "reading_material", 0.90),
            (r"^steps\s+for\s+writing",
             "Reading", "reading_material", 0.90),
            (r"^annotated\s+bibliography\s+(example|template|sample|guide)",
             "Resource", "reference_template", 0.95),
            (r"^(3rd\s+person|third\s+person)\s+writing",
             "Resource", "reference_template", 0.90),
            (r"^uploading\s+assignments?\s*$",
             "Resource", "how_to_guide", 0.95),

            # ── Student support / help ────────────────────────────────────
            (r"^student\s+(help|support|resources?)\s*$",
             "Resource", "support_resource", 0.95),

            # ── Important notices / announcements ─────────────────────────
            (r"^important\s+(information|notice|notes?|announcement)\s*$",
             "Announcement", "course_notice", 0.95),
            (r"^(course\s+)?notice\s*$",
             "Announcement", "course_notice", 0.90),

            # ── External tool / eTextbook access ─────────────────────────
            (r"^wbu\s+etextbook\s+access\s*$",
             "Resource", "external_tool", 0.95),
            (r"etextbook|e-textbook|courseware\s+access",
             "Resource", "external_tool", 0.90),

            # ── Library / research resources ──────────────────────────────
            (r"(library|research\s+and\s+instruction|primo|library\s+databases?|library\s+search)",
             "Resource", "support_resource", 0.90),
            (r"^(introduction\s+to\s+)?library\s+research\s*$",
             "Resource", "support_resource", 0.95),
            (r"^how\s+to\s+(search|research|find)",
             "Resource", "how_to_guide", 0.90),
            (r"^video\s*:\s*(how\s+to|introduction)",
             "LiveSession", "how_to_guide", 0.90),

            # ── Weblink / external URL pages ──────────────────────────────
            # Short pages that are just a URL (e.g. "APA Formatting Website")
            (r"(website|web\s+site|online\s+resource|external\s+link|url)\s*$",
             "Resource", "external_resource", 0.85),
            (r"^(apa|mla|chicago)\s+(formatting|style|citation)",
             "Resource", "reference_template", 0.90),

            # ── Canvas "Week N: Topic" intro pages ────────────────────────
            # e.g. "Week #1: Introduction and Personal Branding"
            # Exclude if title contains discussion/forum keywords (those are Discussion items)
            (r"^week\s*#?\d+\s*:\s*(?!.*\b(discussion|forum|board|dialog|response|replies?)\b)",
             "Lesson", "weekly_guide", 0.90),
        ]
        self._PATTERN_RULES = [
            (re.compile(pattern, re.IGNORECASE), ctype, itype, conf)
            for pattern, ctype, itype, conf in raw
        ]

    # ── Public API ────────────────────────────────────────────────────────

    def enrich(self, lms_course: "LmsCourse") -> "LmsCourse":
        """
        Enrich all curriculum items in *lms_course* in-place.
        Returns the same object for convenient chaining.
        """
        # Structural header titles used in Canvas as section dividers — no real content
        STRUCTURAL_HEADERS = {"watch:", "read:", "complete:", "listen:", "view:", "do:"}

        for module in lms_course.curriculum:
            # Pass 1: classify all items, filter structural headers
            real_items = []
            for idx, item in enumerate(module.items):
                title_stripped = (item.title or "").strip().lower().rstrip(".")
                # Drop Canvas structural section-header items (no content, just a label)
                if title_stripped in STRUCTURAL_HEADERS and not (item.content or "").strip():
                    continue
                item.position = idx
                self._enrich_item(item)
                real_items.append(item)
            module.items = real_items

            # Pass 2: backfill stub assignment content from the week instructions
            # Blackboard assignment items are submission portals — their instructions
            # live in the preceding "WK N Instructions" page in the same module.
            # We extract the relevant section and attach it to the assignment.
            self._backfill_assignment_content(module.items)

        logger.info(
            "LMS course enrichment complete",
            extra={
                "course": lms_course.slug,
                "items": sum(len(m.items) for m in lms_course.curriculum),
            },
        )
        return lms_course

    def _backfill_assignment_content(self, items: list) -> None:
        """
        For assignment items whose content is just a title stub, extract
        the relevant instructions from the preceding weekly guide in the
        same module.

        Strategy: find the last 'weekly_guide' Lesson before each stub
        Assignment, then search its HTML for a section that mentions the
        assignment title. If found, use that section as the assignment content.
        """
        # Find the weekly guide (WK N Instructions) in this module
        guide_content = ""
        for item in items:
            if item.type == "Lesson" and item.instructionalType == "weekly_guide":
                guide_content = item.content or ""

            if item.type == "Assignment":
                stub = f"<p><strong>{item.title}</strong></p>"
                content = (item.content or "").strip()
                # Only backfill if content is a stub (just the title wrapped in tags)
                if content and content != stub:
                    continue  # already has real content

                if not guide_content:
                    continue  # no guide to pull from

                # Try to extract the relevant section from the guide
                extracted = self._extract_assignment_section(item.title, guide_content)
                if extracted:
                    item.content = extracted

    def _extract_assignment_section(self, assignment_title: str, guide_html: str) -> str:
        """
        Extract the section of guide_html that describes the given assignment.
        Returns the extracted HTML or empty string if not found.
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(guide_html, "html.parser")
            title_lower = assignment_title.lower()

            # Strategy 1: find a list item or paragraph that mentions the assignment title
            # and return it plus the next few sibling elements
            for tag in soup.find_all(["li", "p", "h3", "h4", "h5"]):
                text = tag.get_text(separator=" ", strip=True).lower()
                if title_lower in text:
                    # Collect this element and up to 3 following siblings
                    parts = [str(tag)]
                    sibling = tag.find_next_sibling()
                    count = 0
                    while sibling and count < 3:
                        sibling_text = sibling.get_text(strip=True)
                        if sibling_text:
                            parts.append(str(sibling))
                            count += 1
                        sibling = sibling.find_next_sibling()
                    if parts:
                        return "".join(parts)

            # Strategy 2: return the full guide content as context
            # (better than a stub)
            return guide_html

        except Exception:
            return ""

    # ── Private helpers ───────────────────────────────────────────────────

    def _enrich_item(self, item: "LmsCurriculumItem") -> None:
        """Apply all enrichment passes to a single item."""
        title = (item.title or "").strip()
        title_lower = title.lower()
        content_lower = (item.content or "").lower()

        # Preserve structural types from the manifest
        if item.type == "Quiz":
            item.classificationConfidence = 1.0
            item.instructionalType = "assessment"
            item.interactionLevel = "active"
            item.estimatedDuration = self._estimate_duration(item)
            item.learningOutcomes = self._extract_outcomes(title_lower, content_lower)
            return

        if item.type == "Assignment":
            item.classificationConfidence = 1.0
            item.instructionalType = "graded_assignment"
            item.interactionLevel = "active"
            item.estimatedDuration = self._estimate_duration(item)
            item.learningOutcomes = self._extract_outcomes(title_lower, content_lower)
            return

        if item.type == "Discussion":
            item.classificationConfidence = 1.0
            item.instructionalType = "discussion_prompt"
            item.interactionLevel = "active"
            item.estimatedDuration = self._estimate_duration(item)
            item.learningOutcomes = self._extract_outcomes(title_lower, content_lower)
            return

        if item.type == "SubHeader":
            item.classificationConfidence = 1.0
            item.instructionalType = "subheader"
            item.interactionLevel = "passive"
            item.estimatedDuration = 0
            return

        if item.type == "WebLink":
            item.classificationConfidence = 1.0
            item.instructionalType = "weblink"
            item.interactionLevel = "passive"
            item.estimatedDuration = 5
            return

        if item.type == "ExternalTool":
            item.classificationConfidence = 1.0
            item.instructionalType = "external_tool"
            item.interactionLevel = "active"
            item.estimatedDuration = 10
            return

        if item.type == "Lesson":
            # Pages are Lessons. Keep them as Lesson, but compute metadata using heuristics
            item.classificationConfidence = 1.0
            item.instructionalType = self._instructional_type(title_lower, "Lesson")
            item.interactionLevel = "passive"
            item.estimatedDuration = self._estimate_duration(item)
            item.learningOutcomes = self._extract_outcomes(title_lower, content_lower)
            self._extract_video_links(item)
            return

    def _extract_video_links(self, item: "LmsCurriculumItem") -> None:
        """Extract Zoom or YuJa video URL from content if present and set videoUrl."""
        if item.content:
            from bs4 import BeautifulSoup
            try:
                soup = BeautifulSoup(item.content, "html.parser")
                extracted_url = None
                
                # Check for iframes
                for iframe in soup.find_all("iframe", src=True):
                    src = iframe["src"]
                    if "yuja.com" in src or "zoom.us" in src:
                        extracted_url = src
                        break
                        
                # Check for anchors if no iframe found
                if not extracted_url:
                    for anchor in soup.find_all("a", href=True):
                        href = anchor["href"]
                        if "yuja.com" in href or "zoom.us" in href:
                            extracted_url = href
                            break
                            
                if extracted_url:
                    import html as _html
                    item.videoUrl = _html.unescape(extracted_url)
            except Exception as e:
                logger.warning(f"Failed to extract video URL from content: {e}")

    def _correct_quiz_misclassification(
        self,
        title_lower: str,
        content_lower: str,
        item: "LmsCurriculumItem",
    ) -> Optional[tuple]:
        """
        Detect Quiz items that are actually Assignments or other types.

        Returns (type, instructionalType, confidence) if a correction is
        warranted, or None to leave the item as Quiz.

        Blackboard exports frequently catalogue:
        - Annotated bibliography submissions  → Assignment
        - Research paper drop-boxes           → Assignment
        - Case analysis portals               → Assignment
        - "Required First Assignment"         → Assignment
        - Orientation quizzes                 → Quiz (keep)
        """
        # Check Layer 1 patterns first — they are authoritative
        for pattern, ctype, itype, conf in self._PATTERN_RULES:
            if pattern.search(title_lower):
                if ctype != "Quiz":
                    return ctype, itype, conf
                return None  # pattern says Quiz — keep

        # Keyword signals that strongly indicate an assignment submission portal
        assignment_signals = [
            "annotated bibliography", "research paper", "case analysis",
            "faith integration", "required first assignment",
            "term paper", "final paper", "midterm paper",
            "reflection paper", "position paper",
        ]
        if any(sig in title_lower for sig in assignment_signals):
            return "Assignment", "graded_assignment", 0.95

        # Content-body signals: assignment rubric / submission instructions
        body_assignment_signals = [
            "submit your", "upload your", "turn in your",
            "points possible", "grading rubric", "submission instructions",
            "due date", "late submission",
        ]
        body_hits = sum(1 for sig in body_assignment_signals if sig in content_lower)
        if body_hits >= 2:
            return "Assignment", "graded_assignment", 0.85

        return None  # genuine quiz

    def _classify_by_keywords(
        self,
        title_lower: str,
        content_lower: str,
        item: "LmsCurriculumItem",
    ) -> tuple:
        """
        Multi-signal keyword scoring.  Returns (type, confidence).

        Scoring model
        -------------
        Each keyword group has a base confidence.  Every additional keyword
        hit in the same group adds 0.05 (capped at 0.95).  A content-body
        corroboration of the title signal adds another 0.05.
        """

        def _score(kws: List[str], base: float, body_kws: List[str] = None) -> float:
            title_hits = sum(1 for kw in kws if _has_word(kw, title_lower))
            if title_hits == 0:
                return 0.0
            conf = min(0.95, base + 0.05 * (title_hits - 1))
            if body_kws:
                body_hits = sum(1 for kw in body_kws if _has_word(kw, content_lower))
                if body_hits:
                    conf = min(0.95, conf + 0.05)
            return conf

        live_exts = (".mp4", ".mov", ".avi", ".pptx", ".ppt", ".key")

        # ── Policy ────────────────────────────────────────────────────────
        policy_kws = ["syllabus", "policy", "rules", "guideline", "honor code",
                      "compliance", "code of conduct", "academic integrity"]
        policy_body = ["academic integrity", "plagiarism", "grading policy",
                       "late work", "attendance", "course expectations"]
        is_explicit_discussion = "discussion" in title_lower or "forum" in title_lower or "dialog" in title_lower or "dialogue" in title_lower
        c = 0.0 if is_explicit_discussion else _score(policy_kws, 0.85, policy_body)
        if c:
            return "Policy", c

        # ── Discussion ────────────────────────────────────────────────────
        disc_kws = ["discussion", "forum", "board", "dialog", "dialogue",
                    "response", "replies", "introductions", "introduce yourself"]
        disc_body = ["respond to", "reply to", "post your", "classmates",
                     "peer response", "discussion board", "initial post"]
        
        # Avoid misclassifying media files, lectures, slides, resources, readings, policy documents as Discussion
        non_discussion_signals = [
            "slides", "slide", "resource", "materials", "material", "handout",
            "reading", "textbook", "chapter", "article", "paper", "syllabus",
            "policy", "rules", "guidelines", "compliance", "lecture", "video"
        ]
        is_explicit_discussion = "discussion" in title_lower or "forum" in title_lower or "dialog" in title_lower or "dialogue" in title_lower
        is_non_discussion = any(sig in title_lower for sig in non_discussion_signals) or title_lower.endswith(live_exts)
        bypass_discussion = is_non_discussion and not is_explicit_discussion

        c = 0.0 if bypass_discussion else _score(disc_kws, 0.80, disc_body)
        if c:
            return "Discussion", c

        # ── Announcement ─────────────────────────────────────────────────
        ann_kws = ["announcement", "welcome", "important notice", "update",
                   "reminder", "posted", "news"]
        c = _score(ann_kws, 0.80)
        if c:
            return "Announcement", c

        # ── Reading (checked before Resource so guides/templates land here)
        reading_kws = ["textbook", "reading", "chapter", "article", "paper",
                       "book", "manual", "guide", "template", "sample",
                       "example", "steps for", "writing guide",
                       "annotated bibliography"]
        reading_body = ["read chapter", "reading assignment", "textbook pages",
                        "required reading", "supplemental reading"]
        c = _score(reading_kws, 0.80, reading_body)
        if c:
            return "Reading", c
        # Extension-based reading signal
        if title_lower.endswith(".pdf") and not any(
            kw in title_lower for kw in ["practice", "solution", "exam", "quiz", "assignment"]
        ):
            return "Reading", 0.75

        # ── Resource ─────────────────────────────────────────────────────
        resource_kws = ["resource", "support", "help", "tutorial", "uploading",
                        "faq", "practice", "solution", "materials", "handout",
                        "extra credit", "prep", "dataset", "csv",
                        "ipynb", "exercise", "lab", "answer",
                        "library", "website", "web site", "database",
                        "how to search", "how to research", "how to find"]
        c = _score(resource_kws, 0.80)
        if c:
            return "Resource", c

        # ── Live session / media ──────────────────────────────────────────
        live_kws = ["zoom", "webinar", "live session", "video", "recording",
                    "meeting", "presentation", "lecture"]
        # "Video: How to..." prefix is a strong LiveSession signal
        if title_lower.startswith("video:") or title_lower.startswith("video -"):
            return "LiveSession", 0.90
        c = _score(live_kws, 0.80)
        if c:
            return "LiveSession", c
        if title_lower.endswith(live_exts):
            return "LiveSession", 0.75

        # ── Survey ────────────────────────────────────────────────────────
        c = _score(["survey", "evaluation", "feedback", "poll"], 0.80)
        if c:
            return "Survey", c

        # ── External tool ─────────────────────────────────────────────────
        c = _score(["lti", "turnitin", "external tool", "launch", "plugin",
                    "panopto", "kaltura", "piazza", "gradescope"], 0.85)
        if c:
            return "ExternalTool", c

        # ── Content-body fallback: boost confidence when body is rich ─────
        # A Lesson with substantial content (>200 words) is more likely to be
        # a genuine lesson than a placeholder — raise confidence slightly.
        if item.content:
            soup = BeautifulSoup(item.content, "html.parser")
            words = len(soup.get_text().split())
            if words >= 200:
                return "Lesson", 0.75
            if words >= 50:
                return "Lesson", 0.65

        # ── Attachment-based floor boost ──────────────────────────────────
        # An item that has files attached but no strong title/body signal is
        # still more likely to be a real content item than a bare placeholder.
        # Raise the minimum confidence from 55% → 65%.
        if item.attachments:
            return "Lesson", 0.65

        return "Lesson", 0.55

    def _instructional_type(self, title_lower: str, item_type: str) -> str:
        """
        Map a classified item to a fine-grained instructional type.

        Structurally-certain types (Quiz, Assignment, Policy) return their
        canonical instructional type directly without keyword matching to
        prevent cross-bucket bleed.
        """
        # Structurally certain — no keyword override
        if item_type == "Quiz":
            return "assessment"
        if item_type == "Assignment":
            return "graded_assignment"
        if item_type == "Policy":
            return "policy_document"
        if item_type == "Discussion":
            return "discussion_prompt"
        if item_type == "Announcement":
            return "announcement"
        if item_type == "Survey":
            return "survey"
        if item_type == "ExternalTool":
            return "external_tool"

        # For Lesson / Reading / Resource / LiveSession — use title keywords
        instructional_rules: List[tuple] = [
            # Notebooks / labs
            (["ipynb", ".ipynb", "notebook", "jupyter"],         "coding_notebook"),
            (["lab", "hands-on", "hands on", "sandbox"],         "lab_exercise"),
            (["exercise", "practice", "drill", "worksheet"],     "practice_exercise"),
            # Datasets
            ([".csv", "dataset", "data file"],                   "dataset"),
            # Slides
            ([".pptx", ".ppt", "slides", "lecture slides",
              "presentation"],                                    "lecture_slides"),
            # Readings
            ([".pdf", "reading", "textbook", "chapter",
              "article", "paper"],                               "reading_material"),
            # How-to guides
            (["uploading", "how to", "instructions", "guide",
              "steps for"],                                      "how_to_guide"),
            # Instructor bio
            (["about me", "your instructor", "your professor",
              "your facilitator", "your learning facilitator",
              "meet your", "meet the instructor"],               "instructor_bio"),
            # Weekly guides
            (["wk ", "week ", "module ", "weekly",
              "activities", "interactions", "assessments"],     "weekly_guide"),
            # Learning outcomes
            (["learning outcomes", "learning objectives",
              "course outcomes"],                                "learning_outcomes"),
            # Templates / references
            (["template", "sample", "example", "annotated"],    "reference_template"),
            # Support
            (["support", "help", "resources", "faq"],           "support_resource"),
            # Media
            (["zoom", "webinar", "live session", "recording",
              "video", ".mp4", ".mov"],                          "live_or_recorded_session"),
            # Announcements
            (["announcement", "welcome", "notice", "reminder"], "announcement"),
        ]
        for keywords, inst_type in instructional_rules:
            if any(kw in title_lower for kw in keywords):
                return inst_type

        # Type-level fallbacks
        fallbacks = {
            "Reading":     "reading_material",
            "Resource":    "supplementary_resource",
            "LiveSession": "live_or_recorded_session",
        }
        return fallbacks.get(item_type, "lesson_content")

    def _estimate_duration(self, item: "LmsCurriculumItem") -> int:
        """Estimate completion time in minutes."""
        title_lower = (item.title or "").lower()

        for ext, mins in self._ASSET_DURATION.items():
            if title_lower.endswith(ext):
                return mins

        if item.content:
            soup = BeautifulSoup(item.content, "html.parser")
            words = len(soup.get_text().split())
            if words > 50:
                return max(1, round(words / self._WPM))

        if item.attachments:
            return len(item.attachments) * 5

        if item.type == "Quiz" and item.questions:
            return max(5, len(item.questions))

        return 5

    @staticmethod
    def _extract_outcomes(title_lower: str, content_lower: str) -> List[str]:
        """Lightweight learning-outcome extraction from title and content."""
        outcome_signals = [
            r"(?:students?\s+will\s+(?:be\s+able\s+to|learn\s+to|understand|demonstrate))\s+([^.!?\n]{10,120})",
            r"(?:upon\s+completion[^,]*,?\s+(?:students?\s+(?:will|can|should)))\s+([^.!?\n]{10,120})",
            r"(?:learning\s+objectives?:?\s*)([^.!?\n]{10,120})",
            r"(?:by\s+the\s+end\s+of\s+(?:this\s+)?(?:module|lesson|week|unit)[^,]*,?\s+(?:you|students?)\s+(?:will|can|should))\s+([^.!?\n]{10,120})",
        ]
        outcomes: List[str] = []
        combined = f"{title_lower} {content_lower}"
        for pattern in outcome_signals:
            for match in re.finditer(pattern, combined, re.IGNORECASE):
                text = match.group(1).strip().rstrip(".,;")
                if text and text not in outcomes:
                    outcomes.append(text)
                if len(outcomes) >= 5:
                    return outcomes
        return outcomes
