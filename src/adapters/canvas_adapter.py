"""
Canvas API Adapter - Direct extraction from Canvas LMS.

Handles authenticated requests, recursive pagination (Link headers),
and normalization into internal CanvasModel entities.
"""

import os
import requests
import re
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from models.canvas_models import (
    CanvasCourse, CanvasModule, CanvasModuleItem, 
    CanvasPage, CanvasQuiz, CanvasAssignment,
    WorkflowState, QuestionType, SubmissionType,
    CanvasQuestion, CanvasQuestionAnswer
)
from utils.resilience import retry
from observability.logger import get_logger

logger = get_logger(__name__)

class CanvasAdapter:
    """
    Adapter for interacting with the Canvas LMS API.
    Converts API responses into the internal CanvasCourse model structure.
    """

    def __init__(self, base_url: Optional[str] = None, api_token: Optional[str] = None):
        """
        Initialize with Canvas API credentials.
        """
        self.base_url = (base_url or os.getenv("CANVAS_BASE_URL", "")).rstrip('/')
        if not self.base_url.endswith('/api/v1'):
            self.base_url = f"{self.base_url}/api/v1"
        
        self.api_token = api_token or os.getenv("CANVAS_API_TOKEN")
        
        self.headers = {}
        if self.api_token:
            self.headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Accept": "application/json"
            }
        
        # Hardening settings
        self.max_pages = 1000
        self.request_delay = 0.1  # Seconds between paged requests (rate limiting)

    def load(self, payload: Dict[str, Any]) -> CanvasCourse:
        """
        Implementation of the adapter's load interface.
        Payload expected: {"course_id": str}
        """
        course_id = payload.get("course_id")
        if not course_id:
            raise ValueError("course_id is required in payload for CanvasAdapter.")
        return self.fetch_course(course_id)

    def fetch_course(self, course_id: str) -> CanvasCourse:
        """
        Extracts a complete course structure from Canvas API.
        """
        if not self.api_token:
            raise ValueError("CANVAS_API_TOKEN is not configured. Cannot perform Canvas API ingestion.")
            
        logger.info(f"[CanvasAdapter] Extracting Course {course_id} from API")
        
        # 1. Basic Course Info
        course_data = self._get(f"/courses/{course_id}")
        course = CanvasCourse(
            title=course_data.get("name", "Untitled Course"),
            identifier=str(course_data.get("id")),
            created_at=self._parse_date(course_data.get("created_at"))
        )

        # 2. Fetch Modules & Items
        logger.info(f"[CanvasAdapter] Fetching modules and items...")
        modules_data = self._fetch_all(f"/courses/{course_id}/modules?include[]=items")
        for m_data in modules_data:
            module = CanvasModule(
                title=m_data.get("name"),
                identifier=str(m_data.get("id")),
                position=m_data.get("position", 0),
                workflow_state=WorkflowState(m_data.get("workflow_state", "active"))
            )
            
            for i_data in m_data.get("items", []):
                item = CanvasModuleItem(
                    title=i_data.get("title"),
                    identifier=str(i_data.get("id")),
                    content_type=self._map_content_type(i_data.get("type")),
                    indent=i_data.get("indent", 0),
                    position=i_data.get("position"),
                    workflow_state=WorkflowState(i_data.get("workflow_state", "active"))
                )
                module.items.append(item)
            
            course.modules.append(module)

        # 3. Fetch Pages
        logger.info(f"[CanvasAdapter] Fetching pages content...")
        pages_data = self._fetch_all(f"/courses/{course_id}/pages")
        for p_summary in pages_data:
            p_detail = self._get(f"/courses/{course_id}/pages/{p_summary.get('url')}")
            page = CanvasPage(
                title=p_detail.get("title"),
                identifier=str(p_detail.get("url")),
                body=p_detail.get("body", ""),
                workflow_state=WorkflowState(p_detail.get("workflow_state", "active")),
                updated_at=self._parse_date(p_detail.get("updated_at"))
            )
            course.pages.append(page)

        # 4. Fetch Assignments
        logger.info(f"[CanvasAdapter] Fetching assignments...")
        assignments_data = self._fetch_all(f"/courses/{course_id}/assignments")
        for a_data in assignments_data:
            assignment = CanvasAssignment(
                title=a_data.get("name"),
                identifier=str(a_data.get("id")),
                description=a_data.get("description", ""),
                points_possible=float(a_data.get("points_possible", 0.0)),
                due_at=self._parse_date(a_data.get("due_at")),
                workflow_state=WorkflowState(a_data.get("workflow_state", "published") if a_data.get("published") else "unpublished")
            )
            course.assignments.append(assignment)

        # 5. Fetch Quizzes
        logger.info(f"[CanvasAdapter] Fetching quizzes and questions...")
        quizzes_data = self._fetch_all(f"/courses/{course_id}/quizzes")
        for q_data in quizzes_data:
            quiz = CanvasQuiz(
                title=q_data.get("title"),
                identifier=str(q_data.get("id")),
                description=q_data.get("description", ""),
                time_limit=q_data.get("time_limit"),
                allowed_attempts=q_data.get("allowed_attempts", 1),
                shuffle_answers=q_data.get("shuffle_answers", False),
                show_correct_answers=q_data.get("show_correct_answers", True)
            )
            
            questions_data = self._fetch_all(f"/courses/{course_id}/quizzes/{quiz.identifier}/questions")
            for qst_data in questions_data:
                question = self._parse_question(qst_data)
                quiz.questions.append(question)
                
            course.quizzes.append(quiz)

        return course

    def _fetch_all(self, endpoint: str) -> List[Dict[str, Any]]:
        """
        Generic fetcher that handles Canvas API pagination (Link headers).
        Includes safety limits and pacing.
        """
        results = []
        url = f"{self.base_url}{endpoint}" if not endpoint.startswith('http') else endpoint
        
        # Ensure per_page is maximized
        url = self._add_query_param(url, "per_page", "100")

        page_count = 0
        while url and page_count < self.max_pages:
            page_count += 1
            response = self._do_request(url)
            
            data = response.json()
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
                
            # Handle Pagination
            url = self._get_next_link(response.headers.get("Link"))
            
            # Rate limiting / pacing
            if url:
                time.sleep(self.request_delay)
        
        if page_count >= self.max_pages:
            logger.warning(f"[CanvasAdapter] Pagination limit reached ({self.max_pages} pages).")
            
        return results

    @retry(max_attempts=3, base_delay=2, exceptions=(requests.RequestException,))
    def _do_request(self, url: str) -> requests.Response:
        """Internal requester with retry resilience."""
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response

    def _get(self, endpoint: str) -> Dict[str, Any]:
        """Simple GET for single objects with retries."""
        url = f"{self.base_url}{endpoint}"
        response = self._do_request(url)
        return response.json()

    def _add_query_param(self, url: str, key: str, value: str) -> str:
        """Safely add query params to URL."""
        if '?' in url:
            if f'{key}=' not in url:
                return f"{url}&{key}={value}"
            return url
        return f"{url}?{key}={value}"

    def _get_next_link(self, link_header: Optional[str]) -> Optional[str]:
        """
        Parses the 'Link' header to find the 'next' URL.
        Format: <url>; rel="next", <url>; rel="last"
        """
        if not link_header:
            return None
            
        links = link_header.split(',')
        for link in links:
            if 'rel="next"' in link:
                match = re.search(r'<(.*)>', link)
                if match:
                    return match.group(1)
        return None

    def _parse_question(self, qst_data: Dict[str, Any]) -> CanvasQuestion:
        """Helper to parse a single quiz question."""
        question = CanvasQuestion(
            identifier=str(qst_data.get("id")),
            title=qst_data.get("question_name", "Question"),
            question_type=self._map_question_type(qst_data.get("question_type")),
            question_text=qst_data.get("question_text", ""),
            points_possible=float(qst_data.get("points_possible", 1.0)),
            general_feedback=qst_data.get("correct_comments"), # Simplified mapping
            position=qst_data.get("position")
        )
        
        for ans_data in qst_data.get("answers", []):
            answer = CanvasQuestionAnswer(
                id=str(ans_data.get("id")),
                text=ans_data.get("text") or ans_data.get("html", ""),
                weight=float(ans_data.get("weight", 0.0)),
                feedback=ans_data.get("comments")
            )
            question.answers.append(answer)
            
        return question

    def _map_content_type(self, c_type: str) -> str:
        """Map Canvas item types to internal content_type."""
        mapping = {
            "Page": "page",
            "Assignment": "assignment",
            "Quiz": "quiz",
            "DiscussionTopic": "discussion",
            "SubHeader": "subheader",
            "ExternalUrl": "url",
            "File": "file"
        }
        return mapping.get(c_type, "page")

    def _map_question_type(self, c_type: Optional[str]) -> QuestionType:
        """Map Canvas question_type string to Enum."""
        if not c_type:
             return QuestionType.MULTIPLE_CHOICE
        try:
            return QuestionType(c_type)
        except ValueError:
            return QuestionType.MULTIPLE_CHOICE

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Safely parse Canvas ISO dates."""
        if not date_str:
            return None
        try:
            # Handle '2023-01-01T12:00:00Z'
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        except:
            return None
