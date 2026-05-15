#!/usr/bin/env python3
"""
Golden Test Runner - Validates pipeline against known-correct outputs.

Tests the canonical pipeline against the golden dataset to ensure
no regression in content extraction, question parsing, or asset detection.

Usage:
    python scripts/golden_test_runner.py \
        --dataset tests/golden_dataset.json \
        --output validation/golden_test_results.json \
        --fail-on-errors
"""

import argparse
import sys
import os
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from core.canonical_pipeline import CanonicalPipeline
from core.classifier import SourceClassifier
from unittest.mock import patch, MagicMock


@dataclass
class GoldenCourseSpec:
    """Expected results for a golden course."""
    id: str
    path: str
    platform: str
    expected: Dict[str, Any]


class GoldenTestRunner:
    """Runs tests against golden dataset."""
    
    def __init__(self, dataset_path: Path, fail_on_errors: bool = True):
        self.dataset = self._load_dataset(dataset_path)
        self.fail_on_errors = fail_on_errors
        self.results: List[Dict] = []
        
    def _load_dataset(self, path: Path) -> List[GoldenCourseSpec]:
        """Load golden dataset."""
        if not path.exists():
            print(f"[ERROR] Dataset not found: {path}")
            sys.exit(1)
        
        data = json.loads(path.read_text())
        courses = data.get("courses", [])
        return [
            GoldenCourseSpec(
                id=c["id"],
                path=c["path"],
                platform=c["platform"],
                expected=c["expected"]
            )
            for c in courses
        ]
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Execute tests on all golden courses."""
        print(f"\n{'='*60}")
        print(f"GOLDEN TEST SUITE")
        print(f"Running {len(self.dataset)} test cases")
        print(f"{'='*60}")
        
        for course_spec in self.dataset:
            course_path = ROOT / course_spec.path
            if not course_path.exists():
                print(f"[SKIP] {course_spec.id}: path not found")
                continue
            
            result = self._run_single_test(course_spec)
            self.results.append(result)
            
            # Print summary
            score = result["reconciliation"]["score"]
            status_icon = "[PASS]" if score >= 99 else "[WARN]" if score >= 95 else "[FAIL]"
            print(f"{status_icon} {course_spec.id}: {score:.1f}% match")
        
        return self._generate_summary()
    
    def _run_single_test(self, spec: GoldenCourseSpec) -> Dict[str, Any]:
        """Test one course against its expected values."""
        course_path = ROOT / spec.path
        
        # Run canonical pipeline (with mocked DB)
        captured_canonical = None
        
        with patch('core.canonical_pipeline.ChunkedMongoExporter') as MockExporter:
            mock_exporter_instance = MagicMock()
            
            def fake_export(*args, **kwargs):
                nonlocal captured_canonical
                captured_canonical = args[0]
                return f"test_{spec.id}"
                
            mock_exporter_instance.export_canonical_course.side_effect = fake_export
            mock_exporter_instance.close.return_value = None
            MockExporter.return_value = mock_exporter_instance
            
            pipeline = CanonicalPipeline(
                source_path=course_path,
                university_id=os.getenv("DEFAULT_UNIVERSITY_ID", "test"),
                author_id=os.getenv("DEFAULT_AUTHOR_ID", "test")
            )
            
            result = pipeline.run()
            canonical = captured_canonical
        
        # Build test result
        test_result = {
            "course_id": spec.id,
            "path": spec.path,
            "pipeline_success": result.get("status") == "success",
            "expected": spec.expected,
            "actual": {
                "modules": len(canonical.modules),
                "lessons": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Lesson"),
                "assignments": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Assignment"),
                "discussions": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Discussion"),
                "policies": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Policy"),
                "resources": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Resource"),
                "readings": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Reading"),
                "live_sessions": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "LiveSession"),
                "surveys": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Survey"),
                "announcements": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "Announcement"),
                "external_tools": sum(1 for m in canonical.modules for i in m.items if i.content_type.value == "ExternalTool"),
                "assessments": len(canonical.assessments),
                "questions": sum(len(a.questions) for a in canonical.assessments),
                "assets": len(canonical.assets),
            },
            "diffs": {},
            "reconciliation": {"score": 0.0}
        }
        
        # Calculate diffs
        if test_result["pipeline_success"]:
            test_result["diffs"] = self._calculate_diffs(test_result["expected"], test_result["actual"])
            test_result["reconciliation"]["score"] = self._calculate_score(test_result["diffs"])
        else:
            test_result["error"] = result.get("error", "Unknown pipeline failure")
        
        return test_result
    
    def _calculate_diffs(self, expected: Dict, actual: Dict) -> Dict[str, Any]:
        """Calculate differences between expected and actual."""
        diffs = {}
        metrics = [
            "modules", "lessons", "assignments", "discussions", "policies", "resources", "readings",
            "live_sessions", "surveys", "announcements", "external_tools",
            "assessments", "questions", "assets"
        ]
        
        for metric in metrics:
            # Map metric name to semantic category name if needed
            semantic_map = {
                "lessons": "Lesson",
                "assignments": "Assignment",
                "discussions": "Discussion",
                "policies": "Policy",
                "resources": "Resource",
                "readings": "Reading",
                "live_sessions": "LiveSession",
                "surveys": "Survey",
                "announcements": "Announcement",
                "external_tools": "ExternalTool"
            }
            
            if metric in semantic_map:
                exp = expected.get("semantic_classification", {}).get(semantic_map[metric], 0)
            else:
                exp = expected.get(metric, 0)
                
            act = actual.get(metric, 0)
            
            if exp == act:
                status = "exact"
                pct_diff = 0.0
            elif exp == 0:
                # No expectation set but we got data
                status = "unexpected"
                pct_diff = 0.0
            else:
                pct_diff = abs(exp - act) / exp * 100
                if pct_diff <= 2:
                    status = "close"
                elif pct_diff <= 10:
                    status = "minor"
                else:
                    status = "major"
            
            diffs[metric] = {
                "expected": exp,
                "actual": act,
                "pct_diff": round(pct_diff, 1),
                "status": status
            }
        
        # Asset coverage
        if "min_asset_coverage" in expected:
            coverage = (actual["assets"] / expected["assets"]) * 100 if expected["assets"] > 0 else 100
            diffs["asset_coverage"] = {
                "actual_pct": round(coverage, 1),
                "min_required": expected["min_asset_coverage"],
                "status": "ok" if coverage >= expected["min_asset_coverage"] else "critical"
            }
        
        return diffs
    
    def _calculate_score(self, diffs: Dict[str, Any]) -> float:
        """Calculate reconciliation score 0-100."""
        score = 100.0
        
        # Content count accuracy (50% weight)
        count_score = 100
        for metric, diff in diffs.items():
            if metric in ["modules", "lessons", "assessments", "questions"]:
                pct = diff.get("pct_diff", 0)
                if pct > 10:
                    count_score -= 25
                elif pct > 5:
                    count_score -= 15
                elif pct > 2:
                    count_score -= 5
        score = score * 0.5 + count_score * 0.5
        
        # Asset coverage (30% weight)
        if "asset_coverage" in diffs:
            coverage = diffs["asset_coverage"]["actual_pct"]
            asset_score = coverage
            score = score * 0.7 + asset_score * 0.3
        
        return min(100.0, max(0.0, score))
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate overall summary."""
        total = len(self.results)
        if total == 0:
            return {"passed": False, "score": 0.0, "error": "No test results"}
        
        passed_count = sum(1 for r in self.results if r["reconciliation"]["score"] >= 99)
        avg_score = sum(r["reconciliation"]["score"] for r in self.results) / total
        
        summary = {
            "total_tests": total,
            "passed_tests": passed_count,
            "average_score": round(avg_score, 1),
            "all_passed": passed_count == total,
            "details": self.results
        }
        
        # Determine overall status
        if avg_score >= 99 and passed_count == total:
            summary["status"] = "ready"
            summary["passed"] = True
        elif avg_score >= 95:
            summary["status"] = "acceptable"
            summary["passed"] = True
        else:
            summary["status"] = "not_ready"
            summary["passed"] = False
        
        return summary
    
    def save_results(self, results: Dict[str, Any], output_path: Path):
        """Save results as JSON."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(results, indent=2), encoding='utf-8')
        print(f"\n[RESULTS] Saved to: {output_path}")
        
        html_path = output_path.with_suffix('.html')
        self._generate_html(results, html_path)
        print(f"[RESULTS] HTML Report: {html_path}")

    def _generate_html(self, results: Dict[str, Any], output_path: Path):
        """Generate a professional HTML report of the golden test results."""
        status_color = "#10b981" if results.get("passed") else "#ef4444"
        if results.get("status") == "acceptable":
            status_color = "#f59e0b"
            
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='utf-8'>",
            "<title>Golden Test Results</title>",
            "<style>",
            "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; background-color: #f9fafb; }",
            "h1, h2, h3 { color: #111827; margin-top: 0; }",
            f".header-card {{ background-color: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-top: 4px solid {status_color}; }}",
            ".summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-top: 16px; }",
            ".stat-box { background-color: #f3f4f6; padding: 16px; border-radius: 6px; text-align: center; }",
            ".stat-value { font-size: 24px; font-weight: bold; color: #1f2937; }",
            ".stat-label { font-size: 14px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 4px; }",
            ".course-card { background-color: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }",
            "table { width: 100%; border-collapse: collapse; margin-top: 16px; }",
            "th, td { padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }",
            "th { background-color: #f9fafb; font-weight: 600; color: #374151; }",
            ".status-exact { color: #10b981; font-weight: 500; }",
            ".status-close { color: #3b82f6; font-weight: 500; }",
            ".status-minor { color: #f59e0b; font-weight: 500; }",
            ".status-major { color: #ef4444; font-weight: 500; }",
            ".status-unexpected { color: #6b7280; font-weight: 500; }",
            "</style>",
            "</head>",
            "<body>",
            "<div class='header-card'>",
            "<h1>Golden Test Suite Results</h1>",
            f"<p>Status: <strong style='color: {status_color}; text-transform: uppercase;'>{results.get('status', 'UNKNOWN')}</strong></p>",
            "<div class='summary-grid'>",
            f"<div class='stat-box'><div class='stat-value'>{results.get('passed_tests')}/{results.get('total_tests')}</div><div class='stat-label'>Passed Tests</div></div>",
            f"<div class='stat-box'><div class='stat-value'>{results.get('average_score', 0):.1f}%</div><div class='stat-label'>Average Score</div></div>",
            "</div>",
            "</div>"
        ]
        
        semantic_meta = {
            "lessons": {"purpose": "Instructional learning activity", "example": "Lecture video, lecture page", "bg": "#e0f2fe", "color": "#0369a1"},
            "assignments": {"purpose": "Submission-required work", "example": "Essay, upload assignment", "bg": "#fef08a", "color": "#854d0e"},
            "discussions": {"purpose": "Forum/discussion activity", "example": "Weekly discussion", "bg": "#ffedd5", "color": "#c2410c"},
            "policies": {"purpose": "Rules/compliance/course governance", "example": "Syllabus, grading policy", "bg": "#fce7f3", "color": "#be185d"},
            "resources": {"purpose": "Non-graded supporting material", "example": "Help docs, support links", "bg": "#dcfce7", "color": "#15803d"},
            "readings": {"purpose": "Required reading/reference", "example": "PDF chapter, textbook", "bg": "#fae8ff", "color": "#a21caf"},
            "live_sessions": {"purpose": "Synchronous class", "example": "Webinar/Zoom session", "bg": "#fee2e2", "color": "#b91c1c"},
            "surveys": {"purpose": "Feedback/evaluation", "example": "Course evaluation", "bg": "#fef9c3", "color": "#a16207"},
            "announcements": {"purpose": "Time-sensitive instructor post", "example": "Welcome note", "bg": "#e0e7ff", "color": "#4338ca"},
            "external_tools": {"purpose": "LTI/3rd party launch", "example": "Zoom, Turnitin", "bg": "#f3f4f6", "color": "#374151"}
        }

        for detail in results.get("details", []):
            score = detail.get('reconciliation', {}).get('score', 0)
            course_color = "#10b981" if score >= 99 else "#f59e0b" if score >= 95 else "#ef4444"
            
            html.extend([
                f"<div class='course-card' style='border-left: 4px solid {course_color};'>",
                f"<h2>{detail.get('course_id')}</h2>",
                f"<p><strong>Path:</strong> <code>{detail.get('path')}</code></p>",
                f"<p><strong>Match Score:</strong> <span style='color: {course_color}; font-weight: bold;'>{score:.1f}%</span></p>"
            ])
            
            diffs = detail.get('diffs', {})
            core_metrics = ["modules", "assessments", "questions", "assets", "asset_coverage"]
            semantic_metrics = ["lessons", "assignments", "discussions", "policies", "resources", "readings", "live_sessions", "surveys", "announcements", "external_tools"]

            # Core Structure Table
            html.extend([
                "<h3>Core Structure</h3>",
                "<table>",
                "<thead><tr><th>Metric</th><th>Expected</th><th>Actual</th><th>Diff %</th><th>Status</th></tr></thead>",
                "<tbody>"
            ])
            for m in core_metrics:
                data = diffs.get(m)
                if not data: continue
                if m == "asset_coverage":
                    html.append(
                        f"<tr><td>Asset Coverage</td><td>>= {data.get('min_required')}%</td>"
                        f"<td>{data.get('actual_pct')}%</td><td>-</td>"
                        f"<td class='status-{'exact' if data.get('status') == 'ok' else 'major'}'>{data.get('status').upper()}</td></tr>"
                    )
                else:
                    html.append(
                        f"<tr><td>{m.title()}</td><td>{data.get('expected')}</td>"
                        f"<td>{data.get('actual')}</td><td>{data.get('pct_diff')}%</td>"
                        f"<td class='status-{data.get('status', 'minor')}'>{data.get('status', 'UNKNOWN').upper()}</td></tr>"
                    )
            html.extend(["</tbody>", "</table>"])

            # Semantic Classification Table
            html.extend([
                "<h3>Semantic Classification</h3>",
                "<table>",
                "<thead><tr><th>Type</th><th>Purpose</th><th>Example</th><th>Expected</th><th>Actual</th><th>Diff %</th><th>Status</th></tr></thead>",
                "<tbody>"
            ])
            for m in semantic_metrics:
                data = diffs.get(m, {"expected": 0, "actual": 0, "pct_diff": 0, "status": "no_expectation"})
                meta = semantic_meta[m]
                badge = f"<span style='background: {meta['bg']}; color: {meta['color']}; padding: 4px 8px; border-radius: 4px; font-size: 13px; font-weight: 500; white-space: nowrap;'>{m.title().replace('_', ' ')}</span>"
                
                html.append(
                    f"<tr><td>{badge}</td>"
                    f"<td style='color: #4b5563; font-size: 14px;'>{meta['purpose']}</td>"
                    f"<td style='color: #6b7280; font-size: 14px; font-style: italic;'>{meta['example']}</td>"
                    f"<td>{data.get('expected')}</td>"
                    f"<td>{data.get('actual')}</td><td>{data.get('pct_diff')}%</td>"
                    f"<td class='status-{data.get('status', 'minor')}'>{data.get('status', 'UNKNOWN').upper().replace('_', ' ')}</td></tr>"
                )
            html.extend(["</tbody>", "</table>", "</div>"])
            
        html.extend(["</body>", "</html>"])
        
        output_path.write_text('\n'.join(html), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description="Run golden dataset tests")
    parser.add_argument("--dataset", default="tests/golden_dataset.json",
                       help="Path to golden dataset JSON")
    parser.add_argument("--output", default="validation/golden_results.json",
                       help="Output JSON results path")
    parser.add_argument("--fail-on-errors", action="store_true",
                       help="Exit with non-zero code if any test fails")
    args = parser.parse_args()
    
    runner = GoldenTestRunner(Path(args.dataset), fail_on_errors=args.fail_on_errors)
    summary = runner.run_all_tests()
    runner.save_results(summary, Path(args.output))
    
    # Print final verdict
    print(f"\n{'='*60}")
    print(f"GOLDEN TEST SUITE COMPLETE")
    print(f"  Average Score: {summary['average_score']:.1f}%")
    print(f"  Passed: {summary['passed_tests']}/{summary['total_tests']}")
    print(f"  Status: {summary['status'].upper()}")
    
    if summary["status"] == "ready":
        print("\n[PASS] Pipeline validated against golden dataset")
        return 0
    elif summary["status"] == "acceptable":
        print("\n[WARN] Pipeline acceptable but minor issues detected")
        return 0 if not args.fail_on_errors else 1
    else:
        print("\n[FAIL] Pipeline FAILED golden tests - DO NOT DEPLOY")
        return 1


if __name__ == "__main__":
    import os
    sys.exit(main())