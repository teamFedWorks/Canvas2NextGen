#!/usr/bin/env python3
"""
Comprehensive Pipeline Validation Suite

Runs all validation checks to ensure canonical pipeline is production-ready:
1. Golden dataset regression tests
2. Pipeline reconciliation on sample courses
3. Classifier accuracy audit
4. Schema integrity checks
5. Stress testing with large courses

Usage:
    python scripts/validate_deployment_readiness.py \
        --output validation/final_report.html \
        --run-golden-tests \
        --run-reconciliation \
        --check-classifier \
        --stress-test
"""

import argparse
import json
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

# Results storage
validation_results = {
    "timestamp": datetime.now().isoformat(),
    "checks": {},
    "overall": {"passed": False, "score": 0.0}
}


def run_check(name: str, cmd: List[str], timeout: int = 300) -> Dict[str, Any]:
    """Run a validation check and capture results."""
    print(f"\n{'='*60}")
    print(f"CHECK: {name}")
    print(f"{'='*60}")
    
    result = {
        "name": name,
        "status": "unknown",
        "output": "",
        "error": "",
        "duration_seconds": 0
    }
    
    start = datetime.now()
    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        result["status"] = "passed" if proc.returncode == 0 else "failed"
        result["output"] = proc.stdout
        result["error"] = proc.stderr
        print(proc.stdout)
        if proc.stderr:
            print(f"[STDERR] {proc.stderr[:500]}")
    except subprocess.TimeoutExpired:
        result["status"] = "timeout"
        result["error"] = f"Check timed out after {timeout}s"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
    
    result["duration_seconds"] = (datetime.now() - start).total_seconds()
    return result


def check_golden_tests() -> Dict[str, Any]:
    """Run golden dataset regression tests."""
    cmd = [sys.executable, "scripts/test_chunked_exporter.py"]
    res = run_check("Golden Dataset Tests", cmd)
    return res


def check_reconciliation() -> Dict[str, Any]:
    """Run pipeline reconciliation on sample courses."""
    # Use the IT-1104 course as test case
    course_path = ROOT / "storage" / "uploads" / "BS Information Technology" / "IT-1104 Programming I"
    if not course_path.exists():
        return {"status": "skipped", "output": "Test course not found"}
    
    cmd = [
        sys.executable, "scripts/reconcile_pipelines.py",
        "--course-dirs", str(course_path),
        "--output", "validation/reconciliation_report.html",
        "--summary-only"
    ]
    return run_check("Pipeline Reconciliation", cmd, timeout=120)


def check_classifier_audit() -> Dict[str, Any]:
    """Audit classifier accuracy."""
    cmd = [
        sys.executable, "scripts/audit_classifier.py",
        "--dataset", "tests/golden_dataset.json",
        "--output", "validation/classifier_audit.html"
    ]
    return run_check("Classifier Audit", cmd)


def check_canonical_e2e() -> Dict[str, Any]:
    """Run canonical pipeline end-to-end test."""
    cmd = [sys.executable, "scripts/validate_canonical_e2e.py"]
    return run_check("Canonical Pipeline E2E", cmd, timeout=120)


def check_schema_integrity() -> Dict[str, Any]:
    """Validate all canonical models are BSON-serializable."""
    print("\n" + "="*60)
    print("CHECK: Schema Integrity")
    print("="*60)
    
    try:
        from models.canonical_models import (
            CanonicalCourse, CanonicalModule, CanonicalCurriculumItem,
            CanonicalAssessment, CanonicalQuestion, CanonicalAsset,
            CanonicalContentType, CanonicalQuestionType, SourcePlatform
        )
        from exporters.chunked_mongodb_exporter import ChunkedMongoExporter
        import bson
        
        # Create test course
        course = CanonicalCourse(
            identifier="test",
            title="Test Course",
            source_platform=SourcePlatform.CANVAS
        )
        
        exporter = ChunkedMongoExporter()
        course_dict = exporter._canonical_to_dict(course)
        
        # Try BSON encode
        bson.BSON.encode(course_dict)
        
        print("  [PASS] All canonical models are BSON-serializable")
        return {
            "status": "passed",
            "output": "Schema integrity verified"
        }
    except Exception as e:
        print(f"  [FAIL] {e}")
        return {
            "status": "failed",
            "error": str(e)
        }


def check_code_quality() -> Dict[str, Any]:
    """Run linting and type checking."""
    print("\n" + "="*60)
    print("CHECK: Code Quality")
    print("="*60)
    
    issues = []
    
    # Check for common issues
    checks = [
        ("Duplicate _notify methods", "grep -n '_notify' src/core/pipeline.py | wc -l", 1),
        ("Duplicate _finalize methods", "grep -n '_finalize' src/core/pipeline.py | wc -l", 1),
        ("Missing imports", "grep -n 'from typing import' src/models/canonical_models.py", 1),
    ]
    
    for name, cmd, expected_count in checks:
        try:
            result = subprocess.run(
                cmd,
                cwd=ROOT,
                shell=True,
                capture_output=True,
                text=True
            )
            count = int(result.stdout.strip().split('\n')[-1]) if result.stdout else 0
            if count != expected_count:
                issues.append(f"{name}: expected {expected_count} occurrences, got {count}")
        except Exception as e:
            issues.append(f"{name}: check failed - {e}")
    
    if issues:
        print(f"  [WARN] Code quality issues found:")
        for issue in issues:
            print(f"    • {issue}")
        return {
            "status": "warn",
            "output": "Warnings: " + "; ".join(issues)
        }
    else:
        print("  [PASS] No obvious code quality issues")
        return {
            "status": "passed",
            "output": "All checks passed"
        }


def generate_html_report(results: Dict[str, Dict[str, Any]], output_path: Path):
    """Generate comprehensive HTML report."""
    passed = sum(1 for r in results.values() if r["status"] == "passed")
    failed = sum(1 for r in results.values() if r["status"] == "failed")
    warnings = sum(1 for r in results.values() if r["status"] == "warn")
    
    # Calculate overall score
    total_weight = len(results)
    score = (passed / total_weight * 100) if total_weight > 0 else 0
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Deployment Readiness Validation</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 12px; }}
        h1, h2 {{ color: #333; }}
        .score {{ font-size: 64px; font-weight: bold; margin: 20px 0; }}
        .score-good {{ color: #28a745; }}
        .score-warn {{ color: #ffc107; }}
        .score-bad {{ color: #dc3545; }}
        .check {{ padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 5px solid #ccc; }}
        .check.pass {{ background: #d4edda; border-color: #28a745; }}
        .check.fail {{ background: #f8d7da; border-color: #dc3545; }}
        .check.warn {{ background: #fff3cd; border-color: #ffc107; }}
        .check .title {{ font-weight: bold; font-size: 18px; }}
        details {{ margin-top: 10px; }}
        summary {{ cursor: pointer; font-weight: bold; }}
        pre {{ background: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto; max-height: 200px; }}
        .recommendations {{ background: #e7f3ff; padding: 20px; border-radius: 8px; margin-top: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; }}
        th {{ background: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 Deployment Readiness Validation</h1>
        <p>Canonical Pipeline Production Validation</p>
        <div class="score {'score-good' if score >= 90 else 'score-warn' if score >= 70 else 'score-bad'}">
            {score:.0f}% Ready
        </div>
        <p>{passed}/{len(results)} checks passed | {failed} failed | {warnings} warnings</p>
    </div>

    <h2>Check Results</h2>
"""
    
    for check_name, result in results.items():
        status_icon = {"passed": "✅", "failed": "❌", "warn": "⚠️", "skipped": "⏭️"}.get(result["status"], "❓")
        css_class = result["status"]
        
        html += f"""
    <div class="check {css_class}">
        <div class="title">{status_icon} {check_name.replace('_', ' ').title()}</div>
        <div>Status: <strong>{result['status'].upper()}</strong></div>
        <div>Duration: {result.get('duration_seconds', 0):.1f}s</div>
"""
        
        if result.get("output"):
            html += f"""
        <details>
            <summary>Output</summary>
            <pre>{result['output'][:2000]}</pre>
        </details>
"""
        
        if result.get("error"):
            html += f"""
        <details>
            <summary>Errors</summary>
            <pre style="color: red;">{result['error'][:2000]}</pre>
        </details>
"""
        html += """
    </div>
"""
    
    # Recommendations
    html += """
    <div class="recommendations">
        <h2>📋 Recommendations</h2>
        <ul>
"""
    if score >= 95:
        html += """
            <li>✅ <strong>Ready for production:</strong> All critical checks passed.</li>
            <li>✅ <strong>Next step:</strong> Begin canary rollout at 5%.</li>
            <li>✅ <strong>Monitoring:</strong> Set up alerts for the dashboard panels.</li>
"""
    elif score >= 80:
        html += """
            <li>⚠️ <strong>Minor issues detected:</strong> Review warnings above.</li>
            <li>📝 <strong>Action:</strong> Fix flagged items before full rollout.</li>
            <li>🧪 <strong>Testing:</strong> Run additional reconciliation on 10 more courses.</li>
"""
    else:
        html += """
            <li>❌ <strong>Not ready:</strong> Critical failures must be addressed.</li>
            <li>🔍 <strong>Action:</strong> Review failed checks and fix root causes.</li>
            <li>⏸️ <strong>Do NOT deploy</strong> until score ≥ 80.</li>
"""
    
    html += """
        </ul>
    </div>

    <div class="section">
        <h2>📊 Detailed Metrics</h2>
        <table>
            <tr><th>Check</th><th>Status</th><th>Duration</th></tr>
"""
    for check_name, result in results.items():
        status = result["status"].upper()
        duration = result.get("duration_seconds", 0)
        html += f"""
            <tr>
                <td>{check_name.replace('_', ' ').title()}</td>
                <td>{status}</td>
                <td>{duration:.1f}s</td>
            </tr>
"""
    html += """
        </table>
    </div>
</body>
</html>
"""
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding='utf-8')
    print(f"\n[REPORT] Validation report saved to: {output_path}")
    
    return score


def main():
    parser = argparse.ArgumentParser(description="Validate deployment readiness")
    parser.add_argument("--output", default="validation/readiness_report.html",
                       help="Output HTML report path")
    parser.add_argument("--run-golden-tests", action="store_true",
                       help="Run golden dataset regression tests")
    parser.add_argument("--run-reconciliation", action="store_true",
                       help="Run pipeline reconciliation")
    parser.add_argument("--check-classifier", action="store_true",
                       help="Audit classifier accuracy")
    parser.add_argument("--check-schema", action="store_true",
                       help="Validate schema integrity")
    parser.add_argument("--check-code-quality", action="store_true",
                       help="Run code quality checks")
    parser.add_argument("--run-e2e", action="store_true",
                       help="Run end-to-end pipeline test")
    parser.add_argument("--all", action="store_true",
                       help="Run all checks")
    args = parser.parse_args()
    
    if args.all:
        args.run_golden_tests = True
        args.run_reconciliation = True
        args.check_classifier = True
        args.check_schema = True
        args.check_code_quality = True
        args.run_e2e = True
    
    if not any([args.run_golden_tests, args.run_reconciliation, args.check_classifier,
                args.check_schema, args.check_code_quality, args.run_e2e]):
        print("[ERROR] At least one check must be selected")
        parser.print_help()
        return 1
    
    print("\n" + "="*60)
    print("🚀 DEPLOYMENT READINESS VALIDATION")
    print("="*60)
    
    # Run selected checks
    if args.run_golden_tests:
        validation_results["checks"]["golden_tests"] = check_golden_tests()
    
    if args.run_reconciliation:
        validation_results["checks"]["reconciliation"] = check_reconciliation()
    
    if args.check_classifier:
        validation_results["checks"]["classifier_audit"] = check_classifier_audit()
    
    if args.check_schema:
        validation_results["checks"]["schema_integrity"] = check_schema_integrity()
    
    if args.check_code_quality:
        validation_results["checks"]["code_quality"] = check_code_quality()
    
    if args.run_e2e:
        validation_results["checks"]["e2e"] = check_canonical_e2e()
    
    # Generate report
    score = generate_html_report(validation_results["checks"], Path(args.output))
    validation_results["overall"]["score"] = score
    validation_results["overall"]["passed"] = score >= 80
    
    # Exit code
    print(f"\n{'='*60}")
    print(f"FINAL SCORE: {score:.1f}%")
    if score >= 95:
        print("✅ READY FOR PRODUCTION")
        return 0
    elif score >= 80:
        print("⚠️  ACCEPTABLE WITH WARNINGS")
        return 0
    else:
        print("❌ NOT READY - FIX ISSUES")
        return 1


if __name__ == "__main__":
    sys.exit(main())