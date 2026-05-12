"""
Stage 2: Semantic Parsing

Orchestrates all parsers to build complete Canvas course model.
"""

from pathlib import Path
from typing import Optional, Dict
from datetime import datetime

from models.canvas_models import CanvasCourse
from models.migration_report import ParseReport, MigrationError
from parsers.manifest_parser import ManifestParser
from parsers.page_parser import PageParser
from parsers.assignment_parser import AssignmentParser
from parsers.quiz_parser import QuizParser
from parsers.discussion_parser import DiscussionParser
from parsers.weblink_parser import WebLinkParser
from parsers.orphaned_content_handler import OrphanedContentHandler
from parsers.pptx_parser import PptxParser
from observability.logger import get_logger

logger = get_logger(__name__)


class Parser:
    """
    Stage 2: Semantic Parsing
    
    Orchestrates all parsers to build complete CanvasCourse model.
    """
    
    def __init__(self, course_directory: Path):
        """
        Initialize the master Parser.
        
        This constructor sets up the specialized sub-parsers responsible for
        handling different types of Canvas content.
        
        Args:
            course_directory: The root folder containing the unzipped Canvas export.
        """
        self.course_directory = course_directory
        
        # Initialize specialized parsers for each content type.
        # manifest_parser: Reads the main course structure (the map).
        self.manifest_parser = ManifestParser(course_directory)
        
        # page_parser: Handles wiki pages and general text content.
        self.page_parser = PageParser(course_directory)
        
        # assignment_parser: Parses assignment settings and descriptions.
        self.assignment_parser = AssignmentParser(course_directory)
        
        # quiz_parser: Handles complex quiz structures and questions.
        self.quiz_parser = QuizParser(course_directory)
        
        # discussion_parser: Handles course discussions.
        self.discussion_parser = DiscussionParser(course_directory)
        
        # weblink_parser: Handles external web links.
        self.weblink_parser = WebLinkParser(course_directory)
        
        # orphaned_handler: Finds files that exist but aren't listed in the manifest.
        self.orphaned_handler = OrphanedContentHandler(course_directory)
        
        # pptx_parser: A specialized tool for converting PowerPoint XML to HTML pages.
        self.pptx_parser = PptxParser(course_directory)
    
    def parse(self) -> tuple[Optional[CanvasCourse], ParseReport]:
        """
        Orchestrate the parsing of all course components.
        
        This method follows a logical flow: first reading the manifest to build
         the course skeleton, then filling it with content from assignments,
         pages, quizzes, and loose files.
        
        Returns:
            A tuple containing (The built CanvasCourse object, A detailed ParseReport).
        """
        report = ParseReport(timestamp=datetime.now())
        
        # Step 1: Parse manifest (the single source of truth for course structure).
        # This tells us what modules exist and what items belong to them.
        course = self.manifest_parser.parse()
        if course is None:
            # If the manifest is missing or broken, we can't build the course.
            report.errors.extend(self.manifest_parser.errors)
            return None, report
        
        # Step 2: Parse wiki pages AND web_resources HTML pages.
        # Build href → resource-ID map from the manifest so the page parser
        # can key every page (including web_resources HTML files) by its
        # manifest resource identifier rather than its filename stem.
        # This replaces the old post-parse re-keying loop — identifiers are
        # assigned correctly at parse time now.
        resource_href_map: Dict[str, str] = {}
        if course.resources:
            for res_id, resource in course.resources.items():
                if resource.href:
                    resource_href_map[resource.href] = res_id

        pages = self.page_parser.parse_all_pages(resource_href_map=resource_href_map)

        if course.resources:
            # Process PPTX webcontent resources (convert slides → HTML pages)
            for res_id, resource in course.resources.items():
                if resource.type and 'webcontent' in resource.type.lower():
                    if resource.href and resource.href.lower().endswith('.pptx'):
                        file_path = self.course_directory / resource.href
                        if file_path.exists():
                            logger.info("Converting PPTX resource", extra={"path": resource.href})
                            pptx_page = self.pptx_parser.parse_pptx(file_path, identifier=res_id)
                            if pptx_page:
                                pages.append(pptx_page)

        course.pages = pages
        report.pages_parsed = len(pages)
        report.errors.extend(self.page_parser.errors)
        report.errors.extend(self.pptx_parser.errors)

        # Step 3: Parse assignments.
        # Assignments are usually in their own subfolders with metadata and instructions.
        assignments = self.assignment_parser.find_all_assignments()
        course.assignments = assignments
        report.assignments_parsed = len(assignments)
        report.errors.extend(self.assignment_parser.errors)
        
        # Step 4: Parse quizzes.
        # Quizzes involve complex QTI-compliant question parsing.
        quizzes = self.quiz_parser.find_all_quizzes()
        course.quizzes = quizzes
        report.quizzes_parsed = len(quizzes)
        
        # Track the total number of questions extracted across all quizzes.
        total_questions = sum(len(quiz.questions) for quiz in quizzes)
        report.questions_parsed = total_questions
        
        report.errors.extend(self.quiz_parser.errors)
        report.errors.extend(self.quiz_parser.question_parser.errors)
        
        # Step 5: Parse discussions and web links identified in resources.
        # Canvas exports weblinks/discussions in two ways:
        #   (a) resource has href pointing to the XML file  → use it directly
        #   (b) resource has no href (empty) → file is at course_root/{res_id}.xml
        discussions = []
        weblinks = []
        for res_id, resource in course.resources.items():
            if not resource.type:
                continue

            is_discussion = 'discussion' in resource.type.lower() or 'imsdt' in resource.type.lower()
            is_weblink    = 'weblink' in resource.type.lower() or 'imswl' in resource.type.lower()

            if not (is_discussion or is_weblink):
                continue

            # Resolve the file path: use href if present, else fall back to {res_id}.xml
            if resource.href:
                file_path = self.course_directory / resource.href
            else:
                # Canvas often exports these with no href — the file sits at the root
                # named after the resource identifier
                file_path = self.course_directory / f"{res_id}.xml"

            if not file_path.exists():
                continue

            if is_discussion:
                discussion = self.discussion_parser.parse_discussion(file_path)
                if discussion:
                    discussion.identifier = res_id  # Keep manifest ID
                    discussions.append(discussion)
            elif is_weblink:
                weblink = self.weblink_parser.parse_weblink(file_path)
                if weblink:
                    weblink.identifier = res_id  # Keep manifest ID
                    weblinks.append(weblink)
        
        course.discussions = discussions
        course.weblinks = weblinks
        report.errors.extend(self.discussion_parser.errors)
        report.errors.extend(self.weblink_parser.errors)
        
        # Step 6: Process orphaned content.
        # Sometimes there are files in the package that aren't mentioned in the manifest.
        # We find these (like loose slides or PDFs) and put them in a 'Recovered Content' module.
        logger.info("Processing orphaned XML/HTML files")
        # Build the referenced set from:
        #   (a) all resource hrefs in the manifest (covers wiki_content + web_resources HTML)
        #   (b) the {res_id}.xml fallback filenames (weblinks/discussions processed in Step 5)
        # This prevents the orphaned handler from re-processing files already parsed above.
        referenced_files = set()
        for res_id, resource in course.resources.items():
            if resource.href:
                referenced_files.add(resource.href)
            # Always exclude the {res_id}.xml root-level file (weblinks/discussions)
            referenced_files.add(f"{res_id}.xml")
        # Also mark every web_resources HTML file as referenced so the orphaned handler
        # doesn't create duplicate pages for files we already parsed in Step 2.
        web_resources_dir = self.course_directory / "web_resources"
        if web_resources_dir.exists():
            for html_file in web_resources_dir.rglob("*.html"):
                rel = html_file.relative_to(self.course_directory)
                referenced_files.add(str(rel).replace("\\", "/"))
        orphaned_pages = self.orphaned_handler.process_all_orphaned_content(referenced_files)
        
        # Merge discovered orphans into the main course pages collection.
        course.pages.extend(orphaned_pages)
        report.pages_parsed += len(orphaned_pages)
        report.errors.extend(self.orphaned_handler.errors)
        
        return course, report
