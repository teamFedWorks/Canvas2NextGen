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

from models.canonical_models import CanonicalCourse, CanonicalModule, CanonicalCurriculumItem, CanonicalAsset
from observability.logger import get_logger

logger = get_logger(__name__)


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
                
                result.item_count += 1
        
        logger.info("Content enrichment complete",
                   extra={"items": result.item_count, "warnings": len(result.warnings)})
        
        return result