"""
Course Transformer - Maps CanvasCourse models to LmsCourse (MERN LMS) models.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from models.canvas_models import CanvasCourse, CanvasModule, CanvasModuleItem, CanvasPage, CanvasQuiz, CanvasAssignment, CanvasDiscussion, CanvasWebLink
from models.lms_models import (
    LmsCourse, LmsCurriculumModule, LmsCurriculumItem, LmsQuizConfig, 
    LmsAssignmentConfig, LmsGradeSettings, LmsStatus, LmsItemType,
    LmsQuestion, LmsQuestionAnswer
)
from models.migration_report import TransformationReport
from observability.logger import get_logger
from config.lms_schemas import CANVAS_QUESTION_TYPE_MAP
from utils.content_normalizer import normalize_lesson_content

logger = get_logger(__name__)


class CourseTransformer:
    """
    Transforms parsed Canvas data into the custom LMS domain models.
    Ensures alignment with the backend's nested database schema.
    """

    def transform(
        self, 
        canvas_course: CanvasCourse, 
        university_id: Optional[str] = None, 
        author_id: Optional[str] = None,
        course_code: Optional[str] = None,
        department: Optional[str] = None
    ) -> tuple[LmsCourse, TransformationReport]:
        """
        Orchestrates transformation from CanvasCourse to LmsCourse.
        """
        report = TransformationReport()
        logger.info("[CourseTransformer] Starting transformation", extra={"course": canvas_course.title})

        slug = self._slugify(canvas_course.title)
        import os
        lms_course = LmsCourse(
            university=university_id or os.getenv("DEFAULT_UNIVERSITY_ID", "000000000000000000000000"),
            authorId=author_id or os.getenv("DEFAULT_AUTHOR_ID", "000000000000000000000000"),
            authorName="Admin",
            title=canvas_course.title,
            slug=slug,
            courseUrl=slug,
            courseCode=course_code or "IMPORTED",
            department=department or "Imported",
            shortDescription=f"{canvas_course.title} — imported from Canvas LMS",
            description=f"Imported Course: {canvas_course.title}",
            canvas_course_id=canvas_course.identifier
        )

        # Build lookup maps for fast access to content items.
        # Key = identifier used when the content was parsed.
        # Pages are keyed by their stem (filename without extension) from wiki_content/
        # Quizzes and assignments are keyed by their directory name.
        # Discussions and weblinks are keyed by their resource identifier (set in parser.py).
        pages_map = {p.identifier: p for p in canvas_course.pages}
        quizzes_map = {q.identifier: q for q in canvas_course.quizzes}
        assignments_map = {a.identifier: a for a in canvas_course.assignments}
        discussions_map = {d.identifier: d for d in canvas_course.discussions}
        weblinks_map = {w.identifier: w for w in canvas_course.weblinks}

        # Process Modules
        for c_module in canvas_course.modules:
            lms_module = self._transform_module(
                c_module, pages_map, quizzes_map, assignments_map,
                discussions_map, weblinks_map, report
            )
            lms_course.curriculum.append(lms_module)

        logger.info("[CourseTransformer] Transformation complete", extra={
            "modules": len(lms_course.curriculum),
            "errors": len(report.errors)
        })

        return lms_course, report

    def _transform_module(
        self, 
        c_module: CanvasModule, 
        pages_map: Dict[str, CanvasPage],
        quizzes_map: Dict[str, CanvasQuiz],
        assignments_map: Dict[str, CanvasAssignment],
        discussions_map: Dict[str, CanvasDiscussion],
        weblinks_map: Dict[str, CanvasWebLink],
        report: TransformationReport
    ) -> LmsCurriculumModule:
        """Transforms a Canvas module to a curriculum module."""
        lms_module = LmsCurriculumModule(
            title=c_module.title,
            summary="",
            _canvasId=c_module.identifier
        )

        for item in c_module.items:
            lms_item = self._transform_item(
                item, pages_map, quizzes_map, assignments_map,
                discussions_map, weblinks_map, report
            )
            if lms_item:
                # Omit empty/blank items to ensure clean rendering
                is_empty_subheader = (lms_item.type == "SubHeader" and not (lms_item.content or "").strip())
                is_empty_lesson = (lms_item.type == "Lesson" and not (lms_item.content or "").strip() and not getattr(lms_item, "videoUrl", None) and not getattr(item, "content_file", None))
                if is_empty_subheader or is_empty_lesson:
                    logger.info(f"[CourseTransformer] Skipping empty placeholder/subheader item: {lms_item.title}")
                    continue
                lms_module.items.append(lms_item)

        return lms_module

    def _transform_item(
        self, 
        c_item: CanvasModuleItem,
        pages_map: Dict[str, CanvasPage],
        quizzes_map: Dict[str, CanvasQuiz],
        assignments_map: Dict[str, CanvasAssignment],
        discussions_map: Dict[str, CanvasDiscussion],
        weblinks_map: Dict[str, CanvasWebLink],
        report: TransformationReport
    ) -> Optional[LmsCurriculumItem]:
        """Maps a Canvas module item to a unified LmsCurriculumItem."""
        base_item = LmsCurriculumItem(
            title=c_item.title,
            slug=self._slugify(c_item.title),
            _canvasId=c_item.identifier,
            type="Lesson"
        )

        # Use _content_ref (the resource identifierref) for content lookup.
        # Fall back to identifier for backwards compatibility.
        lookup_key = getattr(c_item, '_content_ref', None) or c_item.identifier

        # Carry the content_ref onto the LMS item so AssetUploader can match it
        base_item._content_ref = lookup_key

        if not c_item.content_type:
            # Retain subheader/placeholder items to preserve the exact structure of the manifest (no gaps)
            logger.info("Retaining empty subheader/placeholder item to match manifest structure", extra={"title": c_item.title})
            base_item.content = ""
            base_item.type = "SubHeader"
            return base_item

        if c_item.content_type == 'page':
            page = pages_map.get(lookup_key)
            base_item.type = "Lesson"

            if page:
                # Filter out private SharePoint links with no other content to prevent access issues
                if "sharepoint.com" in page.body.lower():
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(page.body, "html.parser")
                    text = soup.get_text(strip=True)
                    if len(text) < 100:
                        logger.info(f"[CourseTransformer] Skipping private SharePoint page to protect student journey: {c_item.title}")
                        return None

                cleaned_body, video_url = self._extract_and_strip_video(page.body)
                base_item.content = normalize_lesson_content(cleaned_body, title=c_item.title)
                if video_url:
                    base_item.videoUrl = video_url
            # Always return — even without body, the item exists in the module
            # and the AssetUploader will attach any file resources to it.
            return base_item

        elif c_item.content_type == 'quiz':
            quiz = quizzes_map.get(lookup_key)
            if quiz:
                base_item.type = "Quiz"
                # Use description if available; otherwise generate a minimal placeholder
                # so the item isn't flagged as empty by the validator
                if quiz.description and quiz.description.strip():
                    base_item.content = quiz.description
                elif quiz.questions:
                    base_item.content = f"<p>Quiz: {quiz.title} — {len(quiz.questions)} question(s)</p>"
                else:
                    base_item.content = f"<p>Quiz: {quiz.title}</p>"

                # Persist parsed quiz questions (previously discarded).
                mapped_questions: List[LmsQuestion] = []
                for cq in (quiz.questions or []):
                    canvas_qt = getattr(cq, "question_type", None)
                    canvas_qt_value = canvas_qt.value if canvas_qt else None
                    lms_qt = CANVAS_QUESTION_TYPE_MAP.get(canvas_qt_value)
                    if lms_qt is None:
                        # Not renderable in our LMS.
                        continue

                    answers: List[LmsQuestionAnswer] = []
                    for ca in (cq.answers or []):
                        weight = getattr(ca, "weight", 0.0) or 0.0
                        answers.append(
                            LmsQuestionAnswer(
                                id=getattr(ca, "id", ""),
                                text=getattr(ca, "text", "") or "",
                                isCorrect=(float(weight) >= 100.0),
                                feedback=getattr(ca, "feedback", None),
                            )
                        )

                    mapped_questions.append(
                        LmsQuestion(
                            identifier=getattr(cq, "identifier", ""),
                            text=getattr(cq, "question_text", "") or "",
                            type=lms_qt,
                            points=float(getattr(cq, "points_possible", 1.0) if getattr(cq, "points_possible", None) is not None else 1.0),
                            answers=answers,
                            generalFeedback=getattr(cq, "general_feedback", None),
                            position=getattr(cq, "position", None),
                        )
                    )

                base_item.questions = mapped_questions
                base_item.quizConfig = LmsQuizConfig(
                    gradeSettings=LmsGradeSettings(maxScore=quiz.points_possible or 100.0),
                    timeLimit=quiz.time_limit or 60,
                    attemptsAllowed=quiz.allowed_attempts,
                    showCorrectAnswers=quiz.show_correct_answers,
                    requireLockdownBrowser=quiz.require_lockdown_browser,
                    requireLockdownBrowserForResults=quiz.require_lockdown_browser_for_results,
                )
                return base_item
            # Quiz declared in manifest but not parsed — keep as placeholder
            base_item.type = "Quiz"
            return base_item

        elif c_item.content_type == 'assignment':
            assign = assignments_map.get(lookup_key)
            base_item.type = "Assignment"
            if assign:
                instructions = assign.description or ""
                bb_desc = getattr(c_item, '_bb_description', '')
                # If description is just a title stub, try _bb_description
                stub = f"<p><strong>{c_item.title}</strong></p>"
                if bb_desc and (not instructions or instructions.strip() == stub):
                    import html as _html
                    instructions = "<p>" + _html.escape(bb_desc).replace("\n", "</p><p>") + "</p>"
                
                cleaned_inst, video_url = self._extract_and_strip_video(instructions)
                base_item.content = normalize_lesson_content(cleaned_inst or stub, title=c_item.title)
                if video_url:
                    base_item.videoUrl = video_url
                
                base_item.assignmentConfig = LmsAssignmentConfig(
                    gradeSettings=LmsGradeSettings(maxScore=assign.points_possible or 100.0),
                    type="Individual"
                )
            else:
                bb_desc = getattr(c_item, '_bb_description', '')
                if bb_desc:
                    import html as _html
                    cleaned_desc, video_url = self._extract_and_strip_video(
                        "<p>" + _html.escape(bb_desc).replace("\n", "</p><p>") + "</p>"
                    )
                    base_item.content = normalize_lesson_content(cleaned_desc, title=c_item.title)
                    if video_url:
                        base_item.videoUrl = video_url
            return base_item

        elif c_item.content_type == 'discussion':
            discussion = discussions_map.get(lookup_key)
            base_item.type = "Discussion"
            if discussion:
                cleaned_body, video_url = self._extract_and_strip_video(discussion.body)
                base_item.content = normalize_lesson_content(cleaned_body, title=c_item.title)
                if video_url:
                    base_item.videoUrl = video_url
            return base_item

        elif c_item.content_type == 'weblink':
            weblink = weblinks_map.get(lookup_key)
            base_item.type = "WebLink"
            if weblink:
                base_item.content = (
                    f'<p><a href="{weblink.url}" target="_blank" rel="noopener noreferrer">'
                    f'{weblink.title}</a></p>'
                )
            return base_item

        elif c_item.content_type in ('external_tool', 'lti'):
            # LTI / external tool — stored as ExternalTool so the validator
            # can correctly SKIP it (content lives on a third-party platform).
            base_item.type = "ExternalTool"
            body = getattr(c_item, '_bb_body', '')
            if body:
                base_item.content = normalize_lesson_content(body, title=c_item.title)

        elif c_item.content_type == 'file':
            # Standalone file (PDF, DOCX, etc.) from Blackboard csfiles/.
            # The body contains an attachment-wrapper div that the AssetUploader
            # will convert to a CDN URL. We render it as a Resource lesson so
            # the frontend can display it via the attachment viewer.
            base_item.type = "Resource"
            body = getattr(c_item, '_bb_body', '') or ''
            if body:
                base_item.content = normalize_lesson_content(body, title=c_item.title)

        # module item is not silently dropped from the course structure.
        return base_item

    def _slugify(self, text: str) -> str:
        """Standard slug generator."""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        return re.sub(r'[-\s]+', '-', text).strip('-')

    def _extract_and_strip_video(self, html_content: str) -> tuple[str, Optional[str]]:
        """
        Extracts YuJa or Zoom URLs from iframes or links, and removes them from the HTML content.
        Returns (cleaned_html_content, extracted_url).
        """
        if not html_content or ('yuja.com' not in html_content.lower() and 'zoom.us' not in html_content.lower()):
            return html_content, None

        from bs4 import BeautifulSoup
        import html as _html
        
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            extracted_url = None

            # Find and remove YuJa/Zoom iframes
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if "yuja.com" in src or "zoom.us" in src:
                    extracted_url = _html.unescape(src)
                    iframe.decompose()
                    break

            # Find and remove YuJa/Zoom anchors if no iframe was decomposed
            if not extracted_url:
                for anchor in soup.find_all("a", href=True):
                    href = anchor["href"]
                    if "yuja.com" in href or "zoom.us" in href:
                        extracted_url = _html.unescape(href)
                        anchor.decompose()
                        break

            return str(soup), extracted_url
        except Exception as e:
            logger.warning(f"Failed to extract and strip video from content: {e}")
            return html_content, None
