import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.content_normalizer import normalize_lesson_content, repair_text_encoding


def test_repair_text_encoding():
    assert repair_text_encoding("beliefsâ€”and knowledgeâ€¦") == "beliefs-and knowledge..."


def test_plain_text_becomes_structured_html():
    content = "\n".join([
        "Critical Thinking: Facts and Feelings",
        "",
        "CRITICAL THINKING: The systematic evaluation of beliefs.",
        "",
        "1.1 Why It Matters",
        "Our thinking guides our actions.",
        "",
        "Which passage contains an argument?",
        "a. First option",
        "b. Second option",
    ])

    html = normalize_lesson_content(content)

    assert "<h1>Critical Thinking: Facts and Feelings</h1>" in html
    assert "definition-callout" in html
    assert '<span class="section-number">1.1</span>' in html
    assert 'class="review-question"' in html
    assert "<li>First option</li>" in html


def test_existing_html_stays_html_and_is_sanitized():
    html = normalize_lesson_content("<h2>Hello</h2><script>alert(1)</script><p>Worldâ€”ok</p>")

    assert "<h2>Hello</h2>" in html
    assert "<p>World-ok</p>" in html
    assert "<script>" not in html


def test_bullets_split_into_lists():
    content = "Syllabus •Syllabus is available under Syllabus Tab •Please read through " + ("a" * 200)
    html = normalize_lesson_content(content)
    assert "<h1>Syllabus</h1>" in html
    assert "<li>Syllabus is available under Syllabus Tab</li>" in html
    assert "Please read through" in html

