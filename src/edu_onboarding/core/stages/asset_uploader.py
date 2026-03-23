"""
Asset Uploader Stage

Scans HTML content in the LmsCourse model, uploads local file references 
to S3, and rewrites URLs to point to the S3 CDN.
"""

import os
import re
from pathlib import Path
from typing import List, Set, Dict, Tuple, Optional
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

from ...models.lms_models import LmsCourse, LmsModule, LmsLesson, LmsQuiz, LmsAssignment
from ...config.lms_schemas import UPLOADABLE_EXTENSIONS, S3_KEY_TEMPLATE
from ...observability.logger import get_logger
from ...utils.resilience import retry

logger = get_logger(__name__)


class AssetUploader:
    """
    Handles S3 uploads and HTML URL rewriting for course assets.
    """

    def __init__(
        self, 
        course_id: str, 
        source_dir: Path, 
        s3_bucket: str, 
        cdn_base_url: str = "",
        pre_uploaded_assets: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the uploader.
        """
        self.course_id = course_id
        self.source_dir = Path(source_dir)
        self.s3_bucket = s3_bucket
        self.cdn_base_url = cdn_base_url.rstrip('/')
        
        self.s3_client = boto3.client('s3')
        
        # Track uploaded files. Pre-populate for resumability.
        self.uploaded_assets: Dict[str, str] = pre_uploaded_assets or {}
        
        # Stats
        self.stats = {
            "uploaded": 0,
            "skipped": 0,
            "failed": 0
        }

    def process_course(self, lms_course: LmsCourse) -> LmsCourse:
        """
        Iterate through all content in the course and process assets.
        
        Args:
            lms_course: The course model built by the transformer.
            
        Returns:
            The modified LmsCourse with rewritten URLs.
        """
        logger.info("Starting asset upload and URL rewriting", extra={
            "course_id": self.course_id,
            "s3_bucket": self.s3_bucket
        })

        for module in lms_course.modules:
            for lesson in module.lessons:
                lesson.content = self._process_html(lesson.content, lesson.asset_urls)
            
            for quiz in module.quizzes:
                quiz.description = self._process_html(quiz.description)
                for question in quiz.questions:
                    question.text = self._process_html(question.text)
                    for answer in question.answers:
                         if answer.text:
                             answer.text = self._process_html(answer.text)
            
            for assignment in module.assignments:
                assignment.description = self._process_html(assignment.description)

        logger.info("Asset processing complete", extra=self.stats)
        return lms_course

    def _process_html(self, html_content: str, asset_list: Optional[List[str]] = None) -> str:
        """
        Scan HTML for asset tags, upload files to S3, and rewrite paths.
        """
        if not html_content or not isinstance(html_content, str):
            return html_content

        soup = BeautifulSoup(html_content, 'html.parser')
        modified = False

        # 1. Tag-based rewriting
        tags_attrs = [
            ('img', 'src'),
            ('video', 'src'),
            ('source', 'src'),
            ('track', 'src'),
            ('a', 'href'),
            ('iframe', 'src'),
            ('embed', 'src')
        ]

        for tag_name, attr in tags_attrs:
            for tag in soup.find_all(tag_name):
                url = tag.get(attr)
                if url and self._is_local_asset(url):
                    s3_url = self._upload_asset(url)
                    if s3_url:
                        tag[attr] = s3_url
                        modified = True
                        if asset_list is not None:
                            asset_list.append(s3_url)

        # 2. Inline Style Rewriting (e.g., style="background-image: url(...)")
        for tag in soup.find_all(style=True):
            style_content = tag['style']
            new_style, was_changed = self._process_css_text(style_content)
            if was_changed:
                tag['style'] = new_style
                modified = True

        # 3. Internal Style Tag Rewriting
        for style_tag in soup.find_all('style'):
            if style_tag.string:
                new_css, was_changed = self._process_css_text(style_tag.string)
                if was_changed:
                    style_tag.string.replace_with(new_css)
                    modified = True

        return str(soup) if modified else html_content

    def _process_css_text(self, css_text: str) -> Tuple[str, bool]:
        """Scan CSS text for url() patterns and rewrite them."""
        # Regex to find url('...') or url("...") or url(...)
        pattern = r'url\(\s*[\'"]?([^\'"]+?)[\'"]?\s*\)'
        modified = False
        
        def replace_url(match):
            nonlocal modified
            original_url = match.group(1)
            if self._is_local_asset(original_url):
                s3_url = self._upload_asset(original_url)
                if s3_url:
                    modified = True
                    return f"url('{s3_url}')"
            return match.group(0)

        new_css = re.sub(pattern, replace_url, css_text)
        return new_css, modified

    def _is_local_asset(self, url: str) -> bool:
        """Check if a URL refers to a local file in the Canvas package."""
        # Skip absolute URLs, data URIs, and mailtos
        if any(url.startswith(p) for p in ['http://', 'https://', 'data:', 'mailto:', '#']):
            return False
            
        # Check extension
        ext = os.path.splitext(url.split('?')[0])[1].lower()
        return ext in UPLOADABLE_EXTENSIONS

    @retry(max_attempts=3, base_delay=1)
    def _upload_asset(self, relative_path: str) -> Optional[str]:
        """
        Uploads a local file to S3 if not already uploaded.
        Returns the full public/CDN URL.
        """
        # Normalize path
        # Canvas often uses URL-encoded paths like 'images/my%20file.png'
        from urllib.parse import unquote
        clean_path = unquote(relative_path).split('?')[0].lstrip('/')
        
        local_file = self.source_dir / clean_path
        
        if not local_file.exists():
            # Sometimes Canvas references files in subfolders like 'web_resources' or 'media'
            # Let's try to find it in the content inventory if possible, or just skip
            self.stats["skipped"] += 1
            return None

        local_path_str = str(local_file.absolute())
        if local_path_str in self.uploaded_assets:
            return self.uploaded_assets[local_path_str]

        # Construct S3 key
        filename = os.path.basename(clean_path)
        s3_key = S3_KEY_TEMPLATE.format(course_id=self.course_id, filename=filename)
        
        try:
            # Upload to S3
            content_type = self._guess_content_type(local_file)
            self.s3_client.upload_file(
                local_path_str, 
                self.s3_bucket, 
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )
            
            # Construct final URL
            final_url = f"{self.cdn_base_url}/{s3_key}" if self.cdn_base_url else f"https://{self.s3_bucket}.s3.amazonaws.com/{s3_key}"
            
            self.uploaded_assets[local_path_str] = final_url
            self.stats["uploaded"] += 1
            
            logger.debug("Uploaded asset", extra={
                "local": clean_path,
                "s3_key": s3_key,
                "content_type": content_type
            })
            
            return final_url
            
        except ClientError as e:
            logger.error("S3 upload failed", extra={
                "file": clean_path,
                "error": str(e)
            })
            self.stats["failed"] += 1
            return None

    def _guess_content_type(self, path: Path) -> str:
        """Determine MIME type for S3 metadata."""
        import mimetypes
        mtype, _ = mimetypes.guess_type(str(path))
        return mtype or 'application/octet-stream'
