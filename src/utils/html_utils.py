"""
HTML processing and cleaning utilities.

This module provides functions for cleaning, sanitizing, and manipulating HTML content.
"""

import html
import re
from typing import Optional, List
from bs4 import BeautifulSoup
import bleach


def clean_html(content: str) -> str:
    """
    Clean HTML content by unescaping entities and normalizing whitespace.
    
    Args:
        content: Raw HTML content
        
    Returns:
        Cleaned HTML content
    """
    if not content:
        return ""
    
    # Unescape HTML entities
    content = html.unescape(content)
    
    # Normalize whitespace
    content = re.sub(r'\s+', ' ', content)
    content = re.sub(r'\n\s*\n', '\n\n', content)
    
    return content.strip()


def sanitize_html(
    content: str,
    allowed_tags: Optional[List[str]] = None,
    allowed_attributes: Optional[dict] = None
) -> str:
    """
    Sanitize HTML content by removing potentially dangerous elements.
    
    Args:
        content: HTML content to sanitize
        allowed_tags: List of allowed HTML tags (default: common safe tags)
        allowed_attributes: Dict of allowed attributes per tag
        
    Returns:
        Sanitized HTML content
    """
    if not content:
        return ""
    
    # Completely decompose <script> and <style> tags and their contents
    # so their raw JS/CSS text doesn't leak out as plain text.
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']):
            s.decompose()
        content = str(soup)
    except Exception:
        content = re.sub(r'(?is)<script\b[^>]*>.*?</script>', '', content)
        content = re.sub(r'(?is)<style\b[^>]*>.*?</style>', '', content)

    # IMPORTANT:
    # We normalize whitespace for readability, but we must not destroy
    # formatting inside <pre> / <code> (where code indentation matters).
    #
    # Also, we only HTML-unescape outside those blocks to avoid turning
    # escaped code literals (&lt;div&gt;) into actual HTML tags.
    preserve_tokens: List[str] = []

    def _preserve_block(match: re.Match) -> str:
        preserve_tokens.append(match.group(0))
        return f"__PRESERVE_BLOCK_{len(preserve_tokens)-1}__"

    # 1) Mask <pre> and <code> blocks before any unescape/normalization.
    masked = re.sub(r'(?is)<pre\b[^>]*>.*?</pre>', _preserve_block, content)
    masked = re.sub(r'(?is)<code\b[^>]*>.*?</code>', _preserve_block, masked)

    # 2) Unescape entities outside preserved blocks.
    masked = html.unescape(masked)

    # 3) Normalize whitespace outside preserved blocks.
    masked = re.sub(r'\s+', ' ', masked)
    masked = re.sub(r'\n\s*\n', '\n\n', masked)

    # 4) Restore preserved blocks verbatim.
    for i, original in enumerate(preserve_tokens):
        masked = masked.replace(f"__PRESERVE_BLOCK_{i}__", original)

    # Default allowed tags (safe for LMS content)
    if allowed_tags is None:
        allowed_tags = [
            'a', 'abbr', 'acronym', 'b', 'blockquote', 'br', 'code', 'div',
            'em', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'hr', 'i', 'img',
            'li', 'ol', 'p', 'pre', 'span', 'strong', 'table', 'tbody',
            'td', 'th', 'thead', 'tr', 'ul', 'iframe', 'video', 'audio',
            'source', 'figure', 'figcaption'
        ]
    
    # Default allowed attributes
    if allowed_attributes is None:
        allowed_attributes = {
            'a': ['href', 'title', 'target', 'rel'],
            'img': ['src', 'alt', 'title', 'width', 'height'],
            'iframe': ['src', 'width', 'height', 'frameborder', 'allowfullscreen'],
            'video': ['src', 'controls', 'width', 'height'],
            'audio': ['src', 'controls'],
            'source': ['src', 'type'],
            'div': ['class', 'id'],
            'span': ['class', 'id'],
            'p': ['class', 'id'],
            'h1': ['class', 'id'],
            'h2': ['class', 'id'],
            'h3': ['class', 'id'],
            'h4': ['class', 'id'],
            'h5': ['class', 'id'],
            'h6': ['class', 'id'],
            'blockquote': ['class', 'id'],
            'ul': ['class', 'id'],
            'ol': ['class', 'id', 'type'],
            'li': ['class', 'id'],
            'table': ['class', 'id'],
            'td': ['colspan', 'rowspan'],
            'th': ['colspan', 'rowspan'],
        }
    
    # Sanitize using bleach
    sanitized = bleach.clean(
        masked,
        tags=allowed_tags,
        attributes=allowed_attributes,
        strip=True
    )
    
    return sanitized.strip()


def extract_text_from_html(html_content: str) -> str:
    """
    Extract plain text from HTML content.
    
    Args:
        html_content: HTML content
        
    Returns:
        Plain text with HTML tags removed
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(['script', 'style']):
        script.decompose()
    
    # Get text
    text = soup.get_text()
    
    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)
    
    return text


def rewrite_canvas_asset_paths(content: str, base_path: str = "../web_resources/") -> str:
    """
    Rewrite Canvas asset paths from IMS-CC format to actual paths.
    
    Args:
        content: HTML content with Canvas asset references
        base_path: Base path for assets
        
    Returns:
        HTML content with rewritten paths
    """
    if not content:
        return ""
    
    # print(f"DEBUG: Rewriting paths with base_path: {base_path}")
    
    # Replace $IMS-CC-FILEBASE$/ with actual path
    content = re.sub(
        r'\$IMS-CC-FILEBASE\$/([^"\')\s]+)',
        rf'{base_path}\1',
        content
    )
    
    # Replace %24IMS-CC-FILEBASE%24/ (URL encoded version)
    content = re.sub(
        r'%24IMS-CC-FILEBASE%24/([^"\')\s]+)',
        rf'{base_path}\1',
        content
    )
    
    return content


def rewrite_internal_links(
    content: str,
    link_map: dict
) -> str:
    """
    Rewrite internal Canvas links to Tutor LMS links.
    
    Args:
        content: HTML content with Canvas internal links
        link_map: Dictionary mapping Canvas IDs to Tutor IDs/slugs
        
    Returns:
        HTML content with rewritten links
    """
    if not content:
        return ""
    
    soup = BeautifulSoup(content, 'html.parser')
    
    # Find all links
    for link in soup.find_all('a', href=True):
        href = link['href']
        
        # Check if it's a Canvas module item link
        # Format: /courses/{course_id}/modules/items/{item_id}
        match = re.match(r'/courses/\d+/modules/items/(\w+)', href)
        if match:
            item_id = match.group(1)
            if item_id in link_map:
                link['href'] = link_map[item_id]
        
        # Check if it's a Canvas page link
        # Format: /courses/{course_id}/pages/{page_slug}
        match = re.match(r'/courses/\d+/pages/(\w+)', href)
        if match:
            page_slug = match.group(1)
            if page_slug in link_map:
                link['href'] = link_map[page_slug]
    
    return str(soup)


def wrap_in_html_document(title: str, content: str) -> str:
    """
    Wrap content in a complete HTML5 document.
    
    Args:
        title: Document title
        content: HTML content for body
        
    Returns:
        Complete HTML document
    """
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html.escape(title)}</title>
</head>
<body>
{content}
</body>
</html>"""


def extract_images_from_html(html_content: str) -> List[str]:
    """
    Extract all image URLs from HTML content.
    
    Args:
        html_content: HTML content
        
    Returns:
        List of image URLs/paths
    """
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    images = []
    
    for img in soup.find_all('img', src=True):
        images.append(img['src'])
    
    return images


def extract_links_from_html(html_content: str) -> List[str]:
    """
    Extract all links from HTML content.
    
    Args:
        html_content: HTML content
        
    Returns:
        List of URLs
    """
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    links = []
    
    for link in soup.find_all('a', href=True):
        links.append(link['href'])
    
    return links


def is_empty_html(html_content: str) -> bool:
    """
    Check if HTML content is effectively empty (no meaningful content).
    
    Args:
        html_content: HTML content
        
    Returns:
        True if content is empty or contains only whitespace
    """
    if not html_content:
        return True
    
    text = extract_text_from_html(html_content)
    return len(text.strip()) == 0


def get_inner_html(element) -> str:
    """
    Get the inner HTML of an XML/HTML element.
    
    Args:
        element: lxml or BeautifulSoup element
        
    Returns:
        Inner HTML as string
    """
    if element is None:
        return ""
    
    try:
        # Try lxml approach first
        from lxml import etree
        if hasattr(element, 'text'):
            # Get all inner content
            inner = element.text or ""
            for child in element:
                inner += etree.tostring(child, encoding='unicode', method='html')
            return inner
    except:
        pass
    
    try:
        # Try BeautifulSoup approach
        if hasattr(element, 'decode_contents'):
            return element.decode_contents()
    except:
        pass
    
    # Fallback: return text content
    return str(element) if element is not None else ""

def get_body_content(html_content: str) -> str:
    """
    Extract the content inside the <body> tag.
    
    Args:
        html_content: Full HTML string
        
    Returns:
        Content inside body, or empty string if no body tag found
    """
    if not html_content:
        return ""
        
    soup = BeautifulSoup(html_content, 'html.parser')
    if soup.body:
        return soup.body.decode_contents()
    return ""


def basic_markdown_to_html(markdown: str) -> str:
    """
    Convert basic markdown to HTML.
    
    Handles headers, bold, italic, code, lists, links, and paragraphs.
    """
    if not markdown:
        return ""
    
    lines = markdown.split('\n')
    html_parts = []
    in_list = False
    
    for line in lines:
        stripped = line.strip()
        
        # Headers
        if stripped.startswith('### '):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append(f'<h3>{stripped[4:]}</h3>')
        elif stripped.startswith('## '):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append(f'<h2>{stripped[3:]}</h2>')
        elif stripped.startswith('# '):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append(f'<h1>{stripped[2:]}</h1>')
        # List items
        elif stripped.startswith('- ') or stripped.startswith('* '):
            if not in_list:
                html_parts.append('<ul>')
                in_list = True
            html_parts.append(f'<li>{stripped[2:]}</li>')
        elif in_list and not stripped:
            html_parts.append('</ul>')
            in_list = False
        # Empty line
        elif not stripped and in_list:
            html_parts.append('</ul>')
            in_list = False
        # Regular text - handle inline formatting
        elif stripped:
            # Escape HTML first
            text = stripped.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            # Bold
            text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)
            text = re.sub(r'__([^_]+)__', r'<strong>\1</strong>', text)
            # Italic
            text = re.sub(r'\*([^*]+)\*', r'<em>\1</em>', text)
            text = re.sub(r'_([^_]+)_', r'<em>\1</em>', text)
            # Code
            text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
            # Links
            text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
            html_parts.append(f'<p>{text}</p>')
    
    if in_list:
        html_parts.append('</ul>')
    
    return '\n'.join(html_parts)

