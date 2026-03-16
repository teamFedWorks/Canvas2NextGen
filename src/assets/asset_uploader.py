import os
import re
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
from ..utils.resilience import retry

class AssetUploader:
    """
    Scans HTML, uploads assets to S3 in parallel, and rewrites URLs.
    Avoids duplicates using SHA256 hashing.
    """

    def __init__(self, extract_dir: Path, s3_bucket: str, cdn_url: str):
        self.extract_dir = extract_dir
        self.s3_bucket = s3_bucket
        self.cdn_url = cdn_url.rstrip('/')
        self.s3_client = boto3.client('s3')
        self.hash_map = {}  # sha256 -> s3_url

    def process_course_assets(self, course: Dict[str, Any]):
        """
        Iterates through course content and processes all HTML assets.
        """
        with ThreadPoolExecutor(max_workers=10) as executor:
            for module in course.get('curriculum', []):
                for item in module.get('items', []):
                    if item.get('content'):
                        item['content'] = self._process_html(item['content'], executor)

    def _process_html(self, html: str, executor: ThreadPoolExecutor) -> str:
        if not html:
            return html

        soup = BeautifulSoup(html, 'html.parser')
        tasks = []

        # Find img and a tags
        for tag in soup.find_all(['img', 'a']):
            attr = 'src' if tag.name == 'img' else 'href'
            url = tag.get(attr)
            
            if url and self._is_local(url):
                # Submit upload task
                tasks.append((tag, attr, url))

        # We can parallelize the ACTUAL uploads, but for URL rewriting 
        # we need to wait for each. For simplicity in this demo, let's 
        # map urls to future results.
        
        # In a high-perf version, we'd batch the file paths, hash them, 
        # then upload unique ones in parallel.
        
        unique_local_paths = list(set([t[2] for t in tasks]))
        path_to_s3_url = {}

        # Parallelize the unique uploads
        future_to_path = {executor.submit(self._upload_file, path): path for path in unique_local_paths}
        
        for future in future_to_path:
            original_path = future_to_path[future]
            try:
                s3_url = future.result()
                if s3_url:
                    path_to_s3_url[original_path] = s3_url
            except Exception:
                pass

        # Rewrite URLs in soup
        for tag, attr, url in tasks:
            if url in path_to_s3_url:
                tag[attr] = path_to_s3_url[url]

        return str(soup)

    def _is_local(self, url: str) -> bool:
        return not url.startswith(('http://', 'https://', 'mailto:', 'data:'))

    @retry(max_attempts=3, base_delay=2, exceptions=(ClientError,))
    def _perform_s3_upload(self, local_path_str: str, s3_key: str):
        """
        Internal S3 upload helper with retry logic.
        """
        self.s3_client.upload_file(local_path_str, self.s3_bucket, s3_key)

    def _upload_file(self, relative_path: str) -> str:
        # Sanitize path (Canvas often uses URL encoding)
        from urllib.parse import unquote
        clean_path = unquote(relative_path).split('?')[0].lstrip('/')
        local_file = self.extract_dir / clean_path

        if not local_file.exists():
            return None

        # 1. Hashing for deduplication
        file_hash = self._get_hash(local_file)
        if file_hash in self.hash_map:
            return self.hash_map[file_hash]

        # 2. Upload
        ext = local_file.suffix
        s3_key = f"assets/{file_hash}{ext}"
        
        try:
            self._perform_s3_upload(str(local_file), s3_key)
            final_url = f"{self.cdn_url}/{s3_key}"
            self.hash_map[file_hash] = final_url
            return final_url
        except Exception:
            # All retry attempts failed
            return None

    def _get_hash(self, file_path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
