"""
Normalize imported lesson content into semantic HTML.

The importer sometimes receives PDF/OCR text as a plain text blob. This module
keeps real HTML intact, but converts long plain-text readings into structured
HTML so lessons render consistently across student view, faculty preview,
search, and exports.
"""

import html
import re
from typing import List

from bs4 import BeautifulSoup

from utils.html_utils import sanitize_html


MOJIBAKE_REPLACEMENTS = {
    "â€”": "-",
    "â€“": "-",
    "â€œ": '"',
    "â€\u009d": '"',
    "â€˜": "'",
    "â€™": "'",
    "â€¦": "...",
}


def repair_text_encoding(value: str) -> str:
    """Repair common UTF-8-as-Windows-1252 mojibake seen in extracted text."""
    if not value:
        return ""

    repaired = value.replace("\u00a0", " ")
    for bad, good in MOJIBAKE_REPLACEMENTS.items():
        repaired = repaired.replace(bad, good)
    return repaired


def _looks_like_html(value: str) -> bool:
    return bool(re.search(r"</?[a-z][\s\S]*>", value or "", flags=re.IGNORECASE))


def _is_plain_extracted_text(value: str) -> bool:
    if not value or _looks_like_html(value):
        return False

    stripped = value.strip()
    return len(stripped) > 240 or "\n" in stripped


def _merge_wrapped_lines(lines: List[str]) -> List[str]:
    """
    Merge PDF line wraps while preserving headings, questions, options, and
    intentional section boundaries.
    """
    merged: List[str] = []
    current = ""

    def is_boundary(line: str) -> bool:
        return bool(
            re.match(r"^\d+(?:\.\d+)*\s+\S+", line)
            or re.match(r"^[A-Z][A-Z\s:'\"-]{2,}:?\s*$", line)
            or re.match(r"^[a-z]\.\s+", line, flags=re.IGNORECASE)
            or re.match(r"^\[[A-Za-z ]+\]", line)
            or re.match(r"^[A-Z][A-Z\s-]{2,}:\s+\S+", line)
        )

    for raw in lines:
        line = raw.strip()
        if not line:
            if current:
                merged.append(current.strip())
                current = ""
            continue

        if is_boundary(line):
            if current:
                merged.append(current.strip())
            current = line
            if re.match(r"^[a-z]\.\s+", line, flags=re.IGNORECASE):
                merged.append(current.strip())
                current = ""
            continue

        if not current:
            current = line
            continue

        if current.endswith((".", "?", "!", ":", '"')) or line[0].isupper():
            merged.append(current.strip())
            current = line
        else:
            current = f"{current} {line}"

    if current:
        merged.append(current.strip())

    return merged


def _inline_markup(text: str) -> str:
    escaped = html.escape(text)

    escaped = re.sub(
        r"\b(CRITICAL THINKING|ARGUMENT|PREMISE|CONCLUSION|STATEMENT|CLAIM):",
        r"<strong>\1:</strong>",
        escaped,
    )
    escaped = re.sub(r"\[(Premise|Conclusion)\]", r"<strong>[\1]</strong>", escaped)
    return escaped


def plain_text_to_structured_html(value: str, title: str = "") -> str:
    """Convert long plain-text lesson content into semantic LMS HTML."""
    repaired = repair_text_encoding(value)
    lines = _merge_wrapped_lines(repaired.splitlines())
    if not lines:
        return ""

    parts: List[str] = ['<div class="structured-lesson-content">']
    option_buffer: List[str] = []

    def flush_options() -> None:
        nonlocal option_buffer
        if option_buffer:
            parts.append('<ol class="lesson-options" type="a">')
            parts.extend(option_buffer)
            parts.append("</ol>")
            option_buffer = []

    for index, line in enumerate(lines):
        numbered_heading = re.match(r"^(\d+(?:\.\d+)*)\s+(.+)", line)
        definition = re.match(r"^([A-Z][A-Z\s-]{2,}):\s+(.+)", line)
        option = re.match(r"^([a-z])\.\s+(.+)", line, flags=re.IGNORECASE)
        is_first_title = index == 0 and not numbered_heading and not title
        is_section_label = re.match(r"^(QUICK REVIEW|CRITICAL THINKING SKILLS TEST)\b", line, flags=re.IGNORECASE)
        is_all_caps = bool(re.match(r"^[A-Z0-9\s:'\"-]+$", line)) and len(line) <= 90

        if option:
            option_buffer.append(f"<li>{_inline_markup(option.group(2))}</li>")
            continue

        flush_options()

        # Check for bullet separator list pattern
        bullet_chars = ['•', '\u2022', '●', '▪', '\u25cf', '\u25aa']
        has_bullets = any(b in line for b in bullet_chars)

        if has_bullets:
            bullet_pattern = re.compile(r'\s*[•\u2022\u25aa\u25cf\u25e6\u25aa\u25ae\u25ad\u25ac\u25af\u2b24\u25cb]\s*')
            starts_with_bullet = bool(re.match(r'^\s*[•\u2022\u25aa\u25cf\u25e6\u25aa\u25ae\u25ad\u25ac\u25af\u2b24\u25cb]', line))
            sub_parts = [p.strip() for p in bullet_pattern.split(line) if p.strip()]
            if sub_parts:
                if starts_with_bullet:
                    parts.append('<ul>')
                    for p in sub_parts:
                        parts.append(f"<li>{_inline_markup(p)}</li>")
                    parts.append('</ul>')
                else:
                    header_text = sub_parts[0]
                    item_texts = sub_parts[1:]
                    if len(header_text) < 80:
                        if index == 0:
                            parts.append(f"<h1>{_inline_markup(header_text)}</h1>")
                        else:
                            parts.append(f"<h3>{_inline_markup(header_text)}</h3>")
                        if item_texts:
                            parts.append('<ul>')
                            for p in item_texts:
                                parts.append(f"<li>{_inline_markup(p)}</li>")
                            parts.append('</ul>')
                    else:
                        parts.append('<ul>')
                        for p in sub_parts:
                            parts.append(f"<li>{_inline_markup(p)}</li>")
                        parts.append('</ul>')
                continue

        if is_first_title:
            parts.append(f"<h1>{_inline_markup(line)}</h1>")
        elif numbered_heading:
            parts.append(
                f'<h2><span class="section-number">{html.escape(numbered_heading.group(1))}</span> '
                f"{_inline_markup(numbered_heading.group(2))}</h2>"
            )
        elif is_section_label or is_all_caps:
            parts.append(f"<h3>{_inline_markup(line)}</h3>")
        elif definition:
            parts.append(
                '<div class="definition-callout">'
                f"<h4>{html.escape(definition.group(1).title())}</h4>"
                f"<p>{_inline_markup(definition.group(2))}</p>"
                "</div>"
            )
        elif line.startswith('"') and line.endswith('"'):
            quote_text = line[1:-1]
            parts.append(f"<blockquote>{_inline_markup(quote_text)}</blockquote>")
        elif line.endswith("?"):
            parts.append(f'<p class="review-question">{_inline_markup(line)}</p>')
        else:
            parts.append(f"<p>{_inline_markup(line)}</p>")

    flush_options()
    parts.append("</div>")
    return sanitize_html("\n".join(parts))


def normalize_lesson_content(content: str, title: str = "") -> str:
    """
    Return clean semantic HTML for lesson-like content.

    HTML input is sanitized after repairing common mojibake. Plain extracted
    readings are converted into headings, paragraphs, callouts, questions, and
    ordered answer lists.
    """
    if not content:
        return ""

    repaired = repair_text_encoding(content)
    if _is_plain_extracted_text(repaired):
        return plain_text_to_structured_html(repaired, title=title)

    if _looks_like_html(repaired):
        # BeautifulSoup normalizes malformed fragments without stripping tags.
        soup = BeautifulSoup(repaired, "html.parser")
        return sanitize_html(str(soup))

    return sanitize_html(f"<p>{html.escape(repaired.strip())}</p>")
