"""
Asset Uploader Stage

Scans HTML content in the LmsCourse model, migrates both local 
file references and remote Canvas URLs to S3, and rewrites 
URLs to point to the S3 CDN.
"""

import os
import re
import mimetypes
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import tempfile
import shutil
from pathlib import Path
from urllib.parse import urlparse
import ipaddress
import html as html_module
from typing import List, Set, Dict, Tuple, Optional
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    import magic  # type: ignore[import]
except ImportError:
    magic = None

from models.lms_models import LmsCourse, LmsCurriculumModule, LmsCurriculumItem, LmsAttachment
from models.canvas_models import CanvasCourse
from config.lms_schemas import UPLOADABLE_EXTENSIONS, S3_KEY_TEMPLATE
from observability.logger import get_logger

logger = get_logger(__name__)


class AssetUploader:
    """
    Handles S3 uploads and HTML URL rewriting for course assets.
    Supports both local and remote (HTTP) source assets.
    """

    def __init__(
        self, 
        s3_bucket: str,
        course_id: str, 
        source_dir: Optional[Path] = None, 
        cdn_url: str = "",
        institution: str = "SFC",
    ):
        """
        Initialize the uploader.

        Args:
            institution: Institution code (e.g. 'SFC' or 'WBU').  Used as the
                         top-level S3 prefix so assets from different institutions
                         are stored in separate folders.
        """
        self.course_id = course_id
        self.institution = institution or "SFC"
        self.source_dir = source_dir
        self.s3_bucket = s3_bucket
        self.cdn_base_url = (cdn_url or os.getenv("CDN_URL", "")).rstrip('/')
        
        import botocore.client
        config = botocore.client.Config(
            read_timeout=300,
            connect_timeout=300,
            retries={'max_attempts': 3}
        )
        self.s3_client = boto3.client('s3', config=config)
        
        # Auth for remote Canvas assets
        self.api_token = os.getenv("CANVAS_API_TOKEN")
        
        # 1. Initialize Request Session with standard browser headers and retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1, # 1s, 2s, 4s delays
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Track uploaded or failed assets (thread-safe)
        self.uploaded_assets: Dict[str, Optional[str]] = {}
        self._upload_lock = threading.Lock()
        
        self.stats = {"uploaded": 0, "skipped": 0, "failed": 0}
        self._stats_lock = threading.Lock()
        
        # Parallel upload configuration
        self.max_upload_workers = int(os.getenv("MAX_UPLOAD_WORKERS", "10"))
        
        # Phase 1 security hardening (SSRF prevention):
        # Only allow downloading remote assets from your Canvas instance and
        # known CDN/S3 hosts. Everything else is treated as non-downloadable.
        self.allowed_remote_hosts: Set[str] = set()
        self._populate_allowed_remote_hosts()

    def _populate_allowed_remote_hosts(self) -> None:
        # Canvas API base (env may include /api/v1)
        canvas_base_url = os.getenv("CANVAS_BASE_URL", "").strip()
        if canvas_base_url:
            host = (urlparse(canvas_base_url).hostname or "").lower()
            if host:
                self.allowed_remote_hosts.add(host)

        # CDN_URL host
        if self.cdn_base_url:
            cdn_host = (urlparse(self.cdn_base_url).hostname or "").lower()
            if cdn_host:
                self.allowed_remote_hosts.add(cdn_host)

        # S3 origin host(s) for completeness (when CDN_URL isn't configured)
        if self.s3_bucket:
            self.allowed_remote_hosts.add(f"{self.s3_bucket}.s3.amazonaws.com".lower())
            aws_region = os.getenv("AWS_REGION", "").strip()
            if aws_region:
                self.allowed_remote_hosts.add(f"{self.s3_bucket}.s3.{aws_region}.amazonaws.com".lower())

    def _is_allowed_remote_asset_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        if parsed.scheme not in ("http", "https"):
            return False

        host = (parsed.hostname or "").lower()
        if not host:
            return False

        # If allowlist wasn't configured, fail closed.
        if not self.allowed_remote_hosts:
            return False

        # Host must be one of the whitelisted domains/hosts.
        if host not in self.allowed_remote_hosts:
            return False

        # If hostname is an IP literal, block private/reserved ranges.
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        except ValueError:
            # Host is a domain name; allow it (we already allowlisted by hostname).
            pass

        return True

    def process_course_assets(self, lms_course: LmsCourse, canvas_course: Optional[CanvasCourse] = None) -> LmsCourse:
        """
        Full asset migration pass:
          1. Scan HTML content for embedded asset URLs and rewrite to S3.
          2. Upload all manifest webcontent file resources (PDFs, PPTXs, DOCXs, etc.)
             and attach them to the matching curriculum item.
        """
        logger.info(f"Starting asset migration for course {self.course_id}")

        # Pass 1: HTML-embedded assets (images, videos, linked files in content)
        for module in lms_course.curriculum:
            for item in module.items:
                item.content = self._process_html(item.content, item.attachments, canvas_course=canvas_course)

        # Pass 2: Manifest-declared file resources not embedded in HTML
        if canvas_course and canvas_course.resources and self.source_dir:
            self._upload_manifest_resources(lms_course, canvas_course)

        logger.info("Asset migration complete", extra=self.stats)
        return lms_course

    def _upload_manifest_resources(self, lms_course: LmsCourse, canvas_course: CanvasCourse) -> None:
        """
        Upload every webcontent file resource declared in the manifest.
        
        Uses parallel uploads via ThreadPoolExecutor for performance.
        Matching strategy:
          1. Exact _content_ref match (resource identifierref)
          2. Exact filename-stem title match
          3. Falls back to "Resources" item or first module item
        """
        self._upload_manifest_resources_parallel(lms_course, canvas_course)
        # Build lookup: resource_ref -> list of items (multiple items can share a resource)
        ref_map: Dict[str, List[LmsCurriculumItem]] = {}
        title_map: Dict[str, LmsCurriculumItem] = {}

        # Common prefixes to strip for better title matching
        prefixes_to_strip = [
            r'^module\s+\d+\s*[-:]*', 
            r'^week\s+\d+\s*[-:]*', 
            r'^lesson\s+\d+\s*[-:]*',
            r'^chapter\s+\d+\s*[-:]*',
            r'^reading\s*[-:]*'
        ]

        for module in lms_course.curriculum:
            for item in module.items:
                # 1. Map by identifier
                for key in filter(None, [item._canvasId, getattr(item, '_content_ref', None)]):
                    ref_map.setdefault(key, []).append(item)
                
                # 2. Map by cleaned title for exact stem matching
                clean_title = item.title.lower()
                for pattern in prefixes_to_strip:
                    clean_title = re.sub(pattern, '', clean_title).strip()
                
                title_key = re.sub(r'[^\w]', '', clean_title)
                if title_key:
                    title_map[title_key] = item

    def _upload_asset_task(self, href: str, res_id: str, ref_map: Dict, title_map: Dict, local_file: Path) -> Tuple[str, Optional[str], Path]:
        """
        Upload a single asset (for parallel execution).
        
        Returns:
            (href, s3_url_or_None, local_file)
        """
        # Check cache first (thread-safe)
        with self._upload_lock:
            if href in self.uploaded_assets:
                return href, self.uploaded_assets[href], local_file
        
        # Validate extension
        ext = Path(href).suffix.lower()
        if ext not in UPLOADABLE_EXTENSIONS:
            with self._stats_lock:
                self.stats["skipped"] += 1
            return href, None, local_file
        
        # Get upload params
        params = self._get_safe_upload_params(local_file, local_file.name)
        upload_filename = params['filename']
        
        # Perform upload
        logger.info(f"  [S3] Uploading {upload_filename} ({self._human_size(local_file)})")
        s_url = self._perform_s3_upload(
            local_file, 
            filename=upload_filename,
            content_type_override=params['content_type']
        )
        
        # Update cache and stats atomically
        with self._upload_lock:
            self.uploaded_assets[href] = s_url
            if s_url:
                with self._stats_lock:
                    self.stats["uploaded"] += 1
            else:
                with self._stats_lock:
                    self.stats["failed"] += 1
        
        return href, s_url, local_file
    
    def _upload_manifest_resources_parallel(self, lms_course: LmsCourse, canvas_course: CanvasCourse) -> None:
        """
        Parallel version using ThreadPoolExecutor.
        """
        # Build lookup maps (same as before)
        ref_map: Dict[str, List[LmsCurriculumItem]] = {}
        title_map: Dict[str, LmsCurriculumItem] = {}
        
        prefixes_to_strip = [
            r'^module\s+\d+\s*[-:]*', 
            r'^week\s+\d+\s*[-:]*', 
            r'^lesson\s+\d+\s*[-:]*',
            r'^chapter\s+\d+\s*[-:]*',
            r'^reading\s*[-:]*'
        ]
        
        for module in lms_course.curriculum:
            for item in module.items:
                for key in filter(None, [item._canvasId, getattr(item, '_content_ref', None)]):
                    ref_map.setdefault(key, []).append(item)
                clean_title = item.title.lower()
                for pattern in prefixes_to_strip:
                    clean_title = re.sub(pattern, '', clean_title).strip()
                title_key = re.sub(r'[^\w]', '', clean_title)
                if title_key:
                    title_map[title_key] = item
        
        # Build list of upload tasks
        upload_tasks = []
        for res_id, resource in canvas_course.resources.items():
            all_hrefs = []
            if resource.href:
                all_hrefs.append(resource.href)
            if hasattr(resource, 'files') and resource.files:
                all_hrefs.extend(resource.files)
            
            # Deduplicate while preserving order
            seen_hrefs = set()
            for href in all_hrefs:
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                
                ext = Path(href).suffix.lower()
                if ext not in UPLOADABLE_EXTENSIONS:
                    continue
                
                local_file = self.source_dir / href
                if not local_file.exists():
                    alt_href = href.replace(':', '_')
                    alt_file = self.source_dir / alt_href
                    if alt_file.exists():
                        local_file = alt_file
                    else:
                        with self._stats_lock:
                            self.stats["skipped"] += 1
                        continue
                
                # Skip if already uploaded (from HTML pass)
                with self._upload_lock:
                    if href in self.uploaded_assets:
                        continue
                
                upload_tasks.append((href, res_id, local_file))
        
        if not upload_tasks:
            logger.info("  [S3] No manifest resources need upload")
            return
        
        logger.info(f"  [S3] Uploading {len(upload_tasks)} assets in parallel (workers={self.max_upload_workers})")
        
        # Parallel upload
        with ThreadPoolExecutor(max_workers=self.max_upload_workers) as executor:
            future_to_task = {
                executor.submit(self._upload_asset_task, href, res_id, ref_map, title_map, local_file): (href, res_id, local_file)
                for href, res_id, local_file in upload_tasks
            }
            
            # Process results as they complete
            for future in as_completed(future_to_task):
                href, res_id, local_file = future_to_task[future]
                try:
                    _, s_url, _ = future.result()
                    
                    if s_url:
                        # Determine target items and create attachment
                        target_items = ref_map.get(res_id, [])
                        if not target_items:
                            stem_key = re.sub(r'[^\w]', '', local_file.stem.lower())
                            item = title_map.get(stem_key)
                            target_items = [item] if item else []
                        
                        if not target_items:
                            # Find "Resources" fallback
                            resources_item = None
                            for mod in lms_course.curriculum:
                                for it in mod.items:
                                    if it.title.lower() in ('resources', 'course resources', 'materials'):
                                        resources_item = it
                                        break
                                if resources_item:
                                    break
                            if resources_item:
                                target_items = [resources_item]
                            elif lms_course.curriculum and lms_course.curriculum[0].items:
                                target_items = [lms_course.curriculum[0].items[0]]
                            else:
                                logger.debug(f"Asset '{local_file.name}' uploaded but no curriculum item found")
                                continue
                        
                        # Create and attach
                        upload_filename = os.path.basename(urlparse(s_url).path).split('_', 1)[-1]
                        ext_clean = os.path.splitext(upload_filename)[1].upper().strip('.') or "FILE"
                        attachment = LmsAttachment(
                            name=upload_filename,
                            url=s_url,
                            size=self._human_size(local_file),
                            type=ext_clean
                        )
                        
                        for target_item in target_items:
                            if target_item and not any(a.url == s_url for a in target_item.attachments):
                                target_item.attachments.append(attachment)
                                
                except Exception as e:
                    logger.error(f"Error processing upload result for {href}: {e}")
        
        logger.info(f"  [S3] Parallel asset upload complete", extra=self.stats)

    def _human_size(self, path: Path) -> str:
        """Return a human-readable file size string."""
        try:
            size = path.stat().st_size
            for unit in ("B", "KB", "MB", "GB"):
                if size < 1024:
                    return f"{size:.1f}{unit}"
                size /= 1024
            return f"{size:.1f}TB"
        except Exception:
            return "0MB"

    def _process_html(
        self,
        html_content: str,
        asset_list: Optional[List[LmsAttachment]] = None,
        canvas_course: Optional[CanvasCourse] = None,
    ) -> str:
        """
        Scan HTML for asset tags, migrate them to S3, and rewrite paths.
        Populates the attachments list with object metadata.
        """
        if not html_content or not isinstance(html_content, str):
            return html_content

        soup = BeautifulSoup(html_content, 'html.parser')
        modified = False

        def _extract_bbfile_json_from_anchor(a_tag) -> Optional[dict]:
            """
            Blackboard Ultra stores attachment metadata as JSON in a `data-bbfile`
            attribute. In some exports this attribute is malformed like:
              data-bbfile="{"linkName":"file.pdf", ...}"
            which breaks HTML parsing and splits the JSON across attributes.

            We recover by brace-matching on the raw tag HTML.
            """
            try:
                raw = a_tag.get('data-bbfile')
                if isinstance(raw, str):
                    candidate = raw.strip()
                    if candidate.startswith('{') and candidate.endswith('}'):
                        import json
                        return json.loads(candidate)
            except Exception:
                pass

            tag_html = str(a_tag)
            if 'data-bbfile' not in tag_html:
                return None

            start = tag_html.find('{')
            end = tag_html.rfind('}')
            if start == -1 or end == -1 or end <= start:
                return None

            candidate = tag_html[start:end + 1]
            # Blackboard escapes quotes twice: &amp;quot; -> &quot; -> "
            candidate = html_module.unescape(html_module.unescape(candidate)).strip()
            candidate = candidate.replace('&quot;', '"')
            try:
                import json
                return json.loads(candidate)
            except Exception:
                return None

        # Blackboard-specific attachment handling:
        # Blackboard Ultra sometimes stores attachments as an empty anchor with
        # `data-bbfile` JSON metadata. The frontend doesn't understand this,
        # so we convert it into a real `<a href="...">` by uploading the
        # referenced attachment binary from the extracted package.
        bb_title_to_local_href: Dict[str, str] = {}
        if canvas_course and getattr(canvas_course, "resources", None) and self.source_dir:
            for res in canvas_course.resources.values():
                if getattr(res, "title", None) and getattr(res, "href", None):
                    bb_title_to_local_href[str(res.title).lower()] = str(res.href)

        for anchor in soup.find_all('a'):
            if 'data-bbfile' not in str(anchor):
                continue

            bb_file_meta = _extract_bbfile_json_from_anchor(anchor)
            if not bb_file_meta:
                # We'll still try xid-based resolution below (href often contains xid-...).
                bb_file_meta = {}

            file_name = (
                bb_file_meta.get('displayName')
                or bb_file_meta.get('linkName')
                or anchor.get_text(strip=True)
                or "Attachment"
            )
            mime_type = bb_file_meta.get('mimeType') or ""

            # Resolve local binary via Blackboard manifest resource titles OR xid references.
            local_href = None
            for candidate in (bb_file_meta.get('displayName'), bb_file_meta.get('linkName'), file_name):
                if candidate:
                    local_href = bb_title_to_local_href.get(str(candidate).lower())
                    if local_href:
                        break

            # Blackboard Ultra often stores attachments as csfiles entries keyed by xid:
            #   href: bbcswebdav/xid-41004918_1
            # and/or in JSON:
            #   resourceUrl: sessions/.../8-4-1%20file.pdf
            # We can map xid → extracted file path: csfiles/**/__xid-41004918_1.*.
            xid = None
            try:
                tag_html = str(anchor)
                m = re.search(r'xid-(\d+_\d+)', tag_html, flags=re.IGNORECASE)
                if m:
                    xid = m.group(1)
                if not xid:
                    ru = bb_file_meta.get('resourceUrl') or ''
                    m2 = re.search(r'xid-(\d+_\d+)', str(ru), flags=re.IGNORECASE)
                    if m2:
                        xid = m2.group(1)
            except Exception:
                xid = None

            local_path = None
            if self.source_dir and xid:
                try:
                    # Search for Blackboard extracted attachment file.
                    # They appear as: csfiles/.../__xid-<id>_<n>/*.pdf (or direct .pdf).
                    matches = list(self.source_dir.rglob(f"__xid-{xid}*"))[:5]
                    for mp in matches:
                        if mp.is_file():
                            local_path = mp
                            break
                except Exception:
                    local_path = None

            if local_path is None and self.source_dir and local_href:
                local_path = self.source_dir / local_href

            # --- Infer proper file extension and name ---
            params = self._get_safe_upload_params(local_path, file_name, mime_type)
            upload_filename = params['filename']
            mime_type = params['content_type']

            s3_url: Optional[str] = None
            if local_path is not None and local_path.exists():
                s3_url = self._perform_s3_upload(
                    local_path,
                    filename=upload_filename,
                    content_type_override=mime_type,
                )
                if s3_url and asset_list is not None:
                    ext = os.path.splitext(upload_filename)[1].upper().strip('.') or "FILE"
                    asset_list.append(
                        LmsAttachment(
                            name=upload_filename,
                            url=s3_url,
                            size=self._human_size(local_path),
                            type=ext,
                        )
                    )

            # Replace the empty Blackboard anchor with a renderable link.
            # Add data-status='missing' when S3 upload failed so the frontend
            # can style it as a warning card.
            wrapper_attrs: Dict[str, str] = {
                'class': 'attachment-wrapper',
                'data-filename': upload_filename,
                'data-mimetype': str(mime_type),
            }
            if not s3_url:
                wrapper_attrs['data-status'] = 'missing'

            wrapper = soup.new_tag('div', **wrapper_attrs)
            link_attrs: Dict[str, str] = {'class': 'bb-attachment-link'}
            if not s3_url:
                link_attrs['class'] += ' missing-asset'
            link = soup.new_tag(
                'a',
                href=(s3_url or "#"),
                target="_blank",
                **link_attrs,
            )
            # Build human-readable link label using the resolved filename.
            lower_mime = (mime_type or '').lower()
            lower_name = upload_filename.lower()
            if 'pdf' in lower_mime or lower_name.endswith('.pdf'):
                link.string = f"📄 View PDF: {upload_filename}"
            elif 'word' in lower_mime or lower_name.endswith(('.doc', '.docx')):
                link.string = f"📝 Download Word Doc: {upload_filename}"
            elif 'powerpoint' in lower_mime or lower_name.endswith(('.ppt', '.pptx')):
                link.string = f"📊 Download Slides: {upload_filename}"
            elif 'excel' in lower_mime or lower_name.endswith(('.xls', '.xlsx')):
                link.string = f"📊 Download Spreadsheet: {upload_filename}"
            else:
                link.string = f"📎 Download: {upload_filename}"
            wrapper.append(link)

            if not s3_url:
                note = soup.new_tag('span', **{'class': 'attachment-note'})
                note.string = "⚠️ This file could not be resolved during import. Please re-import this course to activate the download."
                wrapper.append(note)

            anchor.replace_with(wrapper)
            modified = True

        tags_attrs = [
            ('img', 'src'), ('video', 'src'), ('source', 'src'),
            ('a', 'href'), ('iframe', 'src'), ('embed', 'src')
        ]

        for tag_name, attr in tags_attrs:
            for tag in soup.find_all(tag_name):
                url = tag.get(attr)
                if not url: continue

                # Determine if asset needs migration
                if self._should_migrate(url):
                    s3_url = self._migrate_asset(url)
                    if s3_url:
                        tag[attr] = s3_url
                        modified = True
                        
                        # Add to attachments if it's a link (downloadable)
                        if asset_list is not None and tag_name == 'a':
                            # Get filename from S3 URL (it has the timestamp_ prefix)
                            s3_filename = os.path.basename(urlparse(s3_url).path).split('_', 1)[-1]
                            ext = os.path.splitext(s3_filename)[1].upper().strip('.')
                            attachment = LmsAttachment(
                                name=tag.get_text() or s3_filename,
                                url=s3_url,
                                size="0MB", # Known limitation for remote assets without HEAD
                                type=ext or "FILE"
                            )
                            # If it was a local file, we can get the real size
                            if not url.startswith('http') and self.source_dir:
                                try:
                                    # Reuse resolution logic to find the file again for size
                                    l_file = self._resolve_local_file(url)
                                    if l_file:
                                        attachment.size = self._human_size(l_file)
                                except Exception:
                                    pass
                            asset_list.append(attachment)

        return str(soup) if modified else html_content

    def _should_migrate(self, url: str) -> bool:
        """Check if URL points to a local file or a remote Canvas asset."""
        if any(url.startswith(p) for p in ['data:', 'mailto:', '#']):
            return False
            
        # If it's on our CDN already, skip
        if self.cdn_base_url and url.startswith(self.cdn_base_url):
            return False
            
        # Remote HTTP assets
        if url.startswith('http'):
            # SSRF protection: only download from whitelisted hosts.
            if not self._is_allowed_remote_asset_url(url):
                return False

            # Don't re-mirror assets that are already on our CDN / S3.
            # (We set href to S3/CDN URLs during earlier steps like Blackboard attachment rewriting.)
            if self.cdn_base_url and url.startswith(self.cdn_base_url):
                return False
            if self.s3_bucket:
                if url.startswith(f"https://{self.s3_bucket}.s3.amazonaws.com/"):
                    return False
                if f"https://{self.s3_bucket}.s3." in url and ".amazonaws.com/" in url:
                    return False
            # Skip external web pages (.html/.htm) — these are hyperlinks in the
            # course content, not downloadable assets.  Mirroring them to S3 is
            # both unnecessary and very slow (45 s timeout per URL).
            ext = os.path.splitext(url.split('?')[0])[1].lower()
            if ext in ('.html', '.htm'):
                return False
            return ext in UPLOADABLE_EXTENSIONS
            
        # Local paths
        return True

    def _migrate_asset(self, path_or_url: str) -> Optional[str]:
        """Orchestrates asset migration from local or remote source."""
        if path_or_url in self.uploaded_assets:
            return self.uploaded_assets[path_or_url]

        if path_or_url.startswith('http'):
            return self._download_and_upload(path_or_url)
        else:
            return self._upload_local(path_or_url)

    def _download_and_upload(self, url: str) -> Optional[str]:
        """Download remote asset and upload to S3."""
        # Double-check cache in case failure was stored during this run
        if url in self.uploaded_assets:
            return self.uploaded_assets[url]

        # SSRF protection: enforce allowlist at download-time too.
        if not self._is_allowed_remote_asset_url(url):
            logger.warning(f"Blocked remote asset download (SSRF): {url}")
            self.stats["skipped"] += 1
            self.uploaded_assets[url] = None
            return None

        temp_file = Path(tempfile.mktemp())
        try:
            headers = {}
            if self.api_token and ('canvas' in url or 'sfc.edu' in url):
                headers["Authorization"] = f"Bearer {self.api_token}"
                
            response = self.session.get(url, headers=headers, stream=True, timeout=45)
            response.raise_for_status()
            
            with open(temp_file, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
            
            # Upload to S3
            filename = os.path.basename(url.split('?')[0])
            logger.info(f"  [S3] Uploading remote asset {filename}")
            s3_url = self._perform_s3_upload(temp_file, filename)
            
            self.uploaded_assets[url] = s3_url
            return s3_url
        except Exception as e:
            logger.error(f"Failed to migrate remote asset {url}: {e}")
            self.stats["failed"] += 1
            # Cache the failure to avoid redundant attempts
            self.uploaded_assets[url] = None
            return None
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def _upload_local(self, relative_path: str) -> Optional[str]:
        """Upload local file from source_dir to S3 with xid resolution fallback."""
        if not self.source_dir:
            return None
            
        local_file = self._resolve_local_file(relative_path)
        if not local_file or not local_file.exists():
            self.stats["skipped"] += 1
            return None

        params = self._get_safe_upload_params(local_file, local_file.name)
        logger.info(f"  [S3] Uploading {params['filename']} ({self._human_size(local_file)})")
        s3_url = self._perform_s3_upload(
            local_file, 
            filename=params['filename'],
            content_type_override=params['content_type']
        )
        self.uploaded_assets[relative_path] = s3_url
        return s3_url

    def _resolve_local_file(self, relative_path: str) -> Optional[Path]:
        """Resolves a relative path (or xid-based Blackboard URL) to a Path object."""
        if not self.source_dir:
            return None

        from urllib.parse import unquote
        clean_path = unquote(relative_path).split('?')[0].lstrip('/')
        
        # 1. Try direct path
        local_file = self.source_dir / clean_path
        if local_file.exists() and local_file.is_file():
            return local_file

        # 2. Try xid-based resolution (common in Blackboard images/links)
        m = re.search(r'xid-(\d+_\d+)', clean_path, flags=re.IGNORECASE)
        if m:
            xid = m.group(1)
            try:
                matches = list(self.source_dir.rglob(f"__xid-{xid}*"))[:5]
                for mp in matches:
                    if mp.is_file():
                        return mp
            except Exception:
                pass

        return None

    def _get_safe_upload_params(self, local_path: Optional[Path], suggested_name: str, mime_type: str = "") -> Dict[str, str]:
        """
        Calculates the safest filename (with extension) and MIME type for an upload.
        Infers extension from MIME type or magic bytes if missing.
        """
        name = str(suggested_name)
        stem, ext = os.path.splitext(name)
        final_mime = mime_type

        if not ext:
            # 1. Try MIME type inference
            inferred_ext = self._ext_from_mime(final_mime)
            
            # 2. Try magic byte sniffing if local path available
            if not inferred_ext and local_path and local_path.exists():
                sniffed = self._sniff_mime(local_path)
                if sniffed:
                    inferred_ext = self._ext_from_mime(sniffed)
                    if not final_mime:
                        final_mime = sniffed
            
            if inferred_ext:
                name = f"{stem}{inferred_ext}"
        
        # If still no MIME type, guess from the (potentially new) filename
        if not final_mime and local_path:
            final_mime = self._guess_content_type(local_path)

        return {
            'filename': name,
            'content_type': final_mime or 'application/octet-stream'
        }

    def _perform_s3_upload(
        self,
        local_path: Path,
        filename: str,
        content_type_override: Optional[str] = None,
    ) -> Optional[str]:
        """Generic S3 upload logic with timestamp prefix."""
        import time
        timestamp = int(time.time() * 1000)
        ts_filename = f"{timestamp}_{filename}"
        s3_key = S3_KEY_TEMPLATE.format(
            institution=self.institution,
            course_id=self.course_id,
            filename=ts_filename,
        )
        try:
            content_type = content_type_override or self._guess_content_type(local_path)
            from boto3.s3.transfer import TransferConfig
            transfer_config = TransferConfig(
                multipart_threshold=8*1024*1024,
                max_concurrency=10,
                multipart_chunksize=8*1024*1024,
                use_threads=True
            )
            self.s3_client.upload_file(
                str(local_path), self.s3_bucket, s3_key,
                ExtraArgs={'ContentType': content_type},
                Config=transfer_config
            )
            final_url = f"{self.cdn_base_url}/{s3_key}" if self.cdn_base_url else f"https://{self.s3_bucket}.s3.amazonaws.com/{s3_key}"
            self.stats["uploaded"] += 1
            return final_url
        except Exception as e:
            logger.error(f"S3 upload failed for {filename}: {e}")
            self.stats["failed"] += 1
            return None

    def _guess_content_type(self, path: Path) -> str:
        mtype, _ = mimetypes.guess_type(str(path))
        return mtype or 'application/octet-stream'

    def _ext_from_mime(self, mime_type: str) -> str:
        """
        Return the preferred file extension for a given MIME type (e.g. '.pdf').
        Returns empty string if the MIME type is unknown or empty.
        """
        if not mime_type:
            return ''
        ext = mimetypes.guess_extension(mime_type.split(';')[0].strip())
        if not ext:
            return ''
        # mimetypes sometimes returns unusual extensions — normalise the common ones.
        _normalise = {
            '.jpe': '.jpg',
            '.jpeg': '.jpg',
            '.htm': '.html',
        }
        return _normalise.get(ext, ext)

    def _sniff_mime(self, path: Path) -> str:
        """
        Attempt to detect the MIME type of a file from its magic bytes.
        """
        if magic:
            try:
                return magic.from_file(str(path), mime=True) or ''
            except Exception:
                pass
        
        # Fallback: extension-based guess
        guessed, _ = mimetypes.guess_type(str(path))
        return guessed or ''
