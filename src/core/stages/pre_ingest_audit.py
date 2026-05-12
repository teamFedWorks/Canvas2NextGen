"""
Pre-Ingest Audit Stage (Fix 5)

Scans the Canvas IMSCC manifest before the full pipeline runs and produces
a fast summary of what the pipeline can and cannot auto-import.

This gives operators realistic expectations upfront and flags courses that
need manual prep work (e.g. Respondus-heavy courses, external-link-only courses).
"""

from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass, field

from config.canvas_schemas import IMS_CC_NAMESPACES, CANVAS_PATHS, CANVAS_RESOURCE_TYPES
from utils.xml_utils import parse_xml_file, find_elements, get_element_attribute
from observability.logger import get_logger

logger = get_logger(__name__)

RESPONDUS_KEYWORDS = ("respondus", "lockdown", "ldb", "proctored")


@dataclass
class PreIngestAuditReport:
    """Summary of what the pipeline can auto-import from this Canvas package."""
    total_items: int = 0
    file_backed_items: int = 0       # webcontent / assignment / quiz with actual files
    external_url_items: int = 0      # imswl_xmlv1p1 — external links
    respondus_quiz_items: int = 0    # quizzes with Respondus keywords in title
    empty_items: int = 0             # items with no resource reference at all
    discussion_items: int = 0        # discussion boards
    estimated_auto_import_pct: float = 0.0
    warnings: list = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_items": self.total_items,
            "file_backed_items": self.file_backed_items,
            "external_url_items": self.external_url_items,
            "respondus_quiz_items": self.respondus_quiz_items,
            "discussion_items": self.discussion_items,
            "empty_items": self.empty_items,
            "estimated_auto_import_pct": round(self.estimated_auto_import_pct, 1),
            "warnings": self.warnings,
        }

    def log_summary(self):
        logger.info(
            "[PreIngestAudit] Summary",
            extra={
                "total_items": self.total_items,
                "file_backed": self.file_backed_items,
                "external_urls": self.external_url_items,
                "respondus": self.respondus_quiz_items,
                "empty": self.empty_items,
                "estimated_auto_pct": f"{self.estimated_auto_import_pct:.0f}%",
            }
        )
        for w in self.warnings:
            logger.warning(f"[PreIngestAudit] {w}")


class PreIngestAuditor:
    """
    Fast manifest scanner — runs before the full pipeline to give operators
    a realistic picture of what will and won't auto-import.
    """

    def __init__(self, course_directory: Path):
        self.course_directory = course_directory
        self.manifest_path = course_directory / CANVAS_PATHS["MANIFEST"]

    def audit(self) -> PreIngestAuditReport:
        report = PreIngestAuditReport()

        root = parse_xml_file(self.manifest_path)
        if root is None:
            report.warnings.append("Could not parse imsmanifest.xml — audit skipped.")
            return report

        # Build resource type map: identifier -> type
        resources = find_elements(root, ".//imscc:resource", IMS_CC_NAMESPACES)
        if not resources:
            resources = find_elements(root, ".//resource", {})

        res_type_map: Dict[str, str] = {}
        for r in resources:
            rid = get_element_attribute(r, "identifier") or ""
            rtype = (get_element_attribute(r, "type") or "").lower()
            if rid:
                res_type_map[rid] = rtype

        # Walk all module child items
        org = root.find(".//{http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1}organization")
        if org is None:
            org = root.find(".//organization")
        if org is None:
            report.warnings.append("No <organization> element found — cannot audit items.")
            return report

        all_items = org.iter("{http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1}item")
        # Also try without namespace
        if not any(True for _ in all_items):
            all_items = org.iter("item")

        # Re-iterate (iter is consumed)
        ns = "http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1"
        item_tag_ns = f"{{{ns}}}item"
        item_tag_bare = "item"

        for elem in org.iter():
            tag = elem.tag
            if tag not in (item_tag_ns, item_tag_bare):
                continue

            # Skip module-level items (those with child items = they are containers)
            children = list(elem)
            has_child_items = any(
                c.tag in (item_tag_ns, item_tag_bare) for c in children
            )
            if has_child_items:
                continue

            identifierref = get_element_attribute(elem, "identifierref") or ""
            title_elem = elem.find(f"{{{ns}}}title") or elem.find("title")
            title = (title_elem.text or "").strip() if title_elem is not None else ""

            report.total_items += 1

            if not identifierref:
                report.empty_items += 1
                continue

            rtype = res_type_map.get(identifierref, "")

            if "imswl" in rtype or "weblink" in rtype:
                report.external_url_items += 1
            elif "discussion" in rtype:
                report.discussion_items += 1
            elif any(k in title.lower() for k in RESPONDUS_KEYWORDS):
                report.respondus_quiz_items += 1
            elif rtype:
                report.file_backed_items += 1
            else:
                report.empty_items += 1

        # Compute estimated auto-import rate
        # External URLs now auto-import (Fix 1 in transformer already handles them)
        # Respondus quizzes cannot auto-import
        auto_importable = report.file_backed_items + report.external_url_items + report.discussion_items
        if report.total_items > 0:
            report.estimated_auto_import_pct = auto_importable / report.total_items * 100

        # Emit warnings for high-risk courses
        if report.respondus_quiz_items > 0:
            report.warnings.append(
                f"{report.respondus_quiz_items} Respondus-locked quiz item(s) detected. "
                "These cannot be auto-imported. Manual re-entry or LTI configuration required."
            )
        if report.external_url_items > 0:
            report.warnings.append(
                f"{report.external_url_items} external URL item(s) detected. "
                "These will be imported as clickable links (no file download)."
            )
        if report.empty_items > 0:
            report.warnings.append(
                f"{report.empty_items} item(s) have no resource reference. "
                "These are likely Canvas navigation placeholders and will be marked SKIP."
            )
        if report.estimated_auto_import_pct < 50:
            report.warnings.append(
                f"Low estimated auto-import rate ({report.estimated_auto_import_pct:.0f}%). "
                "Consider reviewing the Canvas export before ingestion."
            )

        report.log_summary()
        return report
