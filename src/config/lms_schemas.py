"""
Custom LMS Schema Definitions

Mapping tables and defaults for the Canvas -> Custom LMS transformation pipeline.
Replaces tutor_schemas.py.
"""

# ---------------------------------------------------------------------------
# Canvas QTI question type -> Custom LMS question type
# None = skip this question (not renderable in our LMS)
# ---------------------------------------------------------------------------
CANVAS_QUESTION_TYPE_MAP: dict = {
    "multiple_choice_question":         "multiple_choice",
    "multiple_answers_question":        "multiple_choice",   # multi-select treated as MC
    "true_false_question":              "true_false",
    "short_answer_question":            "short_answer",
    "essay_question":                   "essay",
    "fill_in_multiple_blanks_question": "fill_in_blank",
    "matching_question":                "matching",
    "ordering_question":                "ordering",
    "numerical_question":               "short_answer",      # numeric -> free-text fallback
    "calculated_question":              "essay",             # needs manual review
    "formula_question":                 "essay",             # needs manual review
    "file_upload_question":             "essay",             # fallback
    "multiple_dropdowns_question":      "multiple_choice",   # closest equivalent
    "categorization_question":          "matching",          # closest equivalent
    "text_only_question":               None,                # descriptive only - skip
}

# ---------------------------------------------------------------------------
# Canvas workflow_state -> Custom LMS status
# ---------------------------------------------------------------------------
CANVAS_STATUS_MAP: dict = {
    "active":       "published",
    "unpublished":  "draft",
    "deleted":      "archived",
}

# ---------------------------------------------------------------------------
# Default quiz settings applied to every imported quiz
# ---------------------------------------------------------------------------
DEFAULT_QUIZ_SETTINGS: dict = {
    "time_limit_minutes": None,           # No time limit by default
    "attempts_allowed": 1,
    "passing_grade_pct": 60,
    "shuffle_questions": False,
    "show_correct_answers": True,
}

# ---------------------------------------------------------------------------
# Default assignment settings applied to every imported assignment
# ---------------------------------------------------------------------------
DEFAULT_ASSIGNMENT_SETTINGS: dict = {
    "max_file_uploads": 1,
    "max_file_size_mb": 10,
    "submission_types": ["file_upload"],
}

# ---------------------------------------------------------------------------
# Asset file extensions considered as uploadable media
# ---------------------------------------------------------------------------
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".bmp", ".ico"}
VIDEO_EXTENSIONS = {".mp4", ".webm", ".ogv", ".mov", ".avi", ".mkv"}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}
# Additional file types that should be uploaded as downloadable attachments
DOWNLOAD_EXTENSIONS = {".ipynb", ".csv", ".zip", ".txt", ".rb", ".py", ".js", ".json", ".html", ".htm"}
UPLOADABLE_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS | DOCUMENT_EXTENSIONS | DOWNLOAD_EXTENSIONS

# ---------------------------------------------------------------------------
# S3 key prefix template
# Usage: S3_KEY_TEMPLATE.format(institution=..., course_id=..., filename=...)
# Assets are stored under {institution}/{course_id}/ so SFC and WBU assets
# are cleanly separated in the CDN bucket.
# ---------------------------------------------------------------------------
S3_KEY_TEMPLATE = "{institution}/{course_id}/{filename}"
