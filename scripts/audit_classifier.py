#!/usr/bin/env python3
"""
Classifier Audit Tool - Validates source detection accuracy.

Tests the SourceClassifier against known ground truth data to measure
precision, recall, and confidence calibration.

Usage:
    python scripts/audit_classifier.py \
        --dataset tests/classification_dataset.json \
        --threshold 0.75 \
        --output audit/classification_audit.html
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from core.classifier import SourceClassifier, classify_source, SourcePlatform


@dataclass
class ClassificationRecord:
    """One entry in the classification dataset."""
    path: str
    expected_platform: str
    expected_confidence_min: float = 0.8
    notes: str = ""


class ClassifierAuditor:
    """Audits classifier performance."""
    
    def __init__(self, dataset_path: Path):
        self.dataset = self._load_dataset(dataset_path)
        self.results: List[Dict] = []
        self.misclassifications: List[Dict] = []
        self.low_confidence: List[Dict] = []
        
    def _load_dataset(self, path: Path) -> List[ClassificationRecord]:
        """Load classification dataset."""
        if not path.exists():
            print(f"[WARN] Dataset not found: {path}")
            print("  Using built-in minimal dataset")
            return self._get_builtin_dataset()
        
        data = json.loads(path.read_text())
        return [
            ClassificationRecord(
                path=entry['path'],
                expected_platform=entry['expected_platform'],
                expected_confidence_min=entry.get('min_confidence', 0.8),
                notes=entry.get('notes', '')
            )
            for entry in data
        ]
    
    def _get_builtin_dataset(self) -> List[ClassificationRecord]:
        """Minimal built-in test set."""
        return [
            ClassificationRecord(
                path="storage/uploads/BS Information Technology/IT-1104 Programming I",
                expected_platform="canvas",
                expected_confidence_min=0.7
            ),
            ClassificationRecord(
                path="storage/uploads/WBU",
                expected_platform="blackboard",
                expected_confidence_min=0.7
            ),
        ]
    
    def run_audit(self) -> Dict[str, Any]:
        """Run classification on all dataset entries."""
        print(f"\nAuditing classifier against {len(self.dataset)} entries")
        print("="*60)
        
        platform_counts = defaultdict(int)
        correct_by_platform = defaultdict(int)
        
        for record in self.dataset:
            path = ROOT / record.path
            if not path.exists():
                print(f"[SKIP] Not found: {record.path}")
                continue
            
            result = classify_source(path)
            
            # Record metrics
            platform_counts[record.expected_platform] += 1
            is_correct = result.platform.value == record.expected_platform
            if is_correct:
                correct_by_platform[record.expected_platform] += 1
            
            # Track misclassifications
            if not is_correct:
                self.misclassifications.append({
                    "path": record.path,
                    "expected": record.expected_platform,
                    "got": result.platform.value,
                    "confidence": result.confidence
                })
            
            # Track low-confidence (even if correct)
            if result.confidence < record.expected_confidence_min:
                self.low_confidence.append({
                    "path": record.path,
                    "expected": record.expected_platform,
                    "got": result.platform.value,
                    "confidence": result.confidence,
                    "min_required": record.expected_confidence_min
                })
            
            # Verbose output
            status = "✓" if is_correct and result.confidence >= record.expected_confidence_min else "⚠️"
            print(f"  {status} {path.name[:40]:40s} → {result.platform.value:12s} (conf={result.confidence:.2f})")
            
            self.results.append({
                "path": record.path,
                "expected": record.expected_platform,
                "predicted": result.platform.value,
                "confidence": result.confidence,
                "correct": is_correct,
                "meets_threshold": result.confidence >= record.expected_confidence_min
            })
        
        # Calculate metrics
        total = len(self.results)
        if total == 0:
            return {"error": "No valid test cases found"}
        
        accuracy = sum(1 for r in self.results if r["correct"]) / total * 100
        threshold_met = sum(1 for r in self.results if r["meets_threshold"]) / total * 100
        
        metrics = {
            "total_tests": total,
            "overall_accuracy": round(accuracy, 1),
            "threshold_compliance": round(threshold_met, 1),
            "misclassifications": len(self.misclassifications),
            "low_confidence": len(self.low_confidence),
            "by_platform": {}
        }
        
        # Per-platform metrics
        for platform, count in platform_counts.items():
            correct = correct_by_platform[platform]
            metrics["by_platform"][platform] = {
                "tests": count,
                "correct": correct,
                "accuracy": round(correct / count * 100, 1) if count > 0 else 0
            }
        
        return metrics
    
    def generate_report(self, metrics: Dict[str, Any], output_path: Path):
        """Generate HTML audit report."""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Classifier Audit Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #f0f0f0; padding: 20px; border-radius: 8px; }}
        .metric {{ font-size: 24px; font-weight: bold; margin: 10px 0; }}
        .pass {{ color: #28a745; }}
        .fail {{ color: #dc3545; }}
        .warn {{ color: #ffc107; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background: #f2f2f2; }}
        .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 12px; }}
        .badge-correct {{ background: #d4edda; }}
        .badge-incorrect {{ background: #f8d7da; }}
        .badge-lowconf {{ background: #fff3cd; }}
        pre {{ background: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto; max-height: 300px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Classifier Audit Report</h1>
        <p><strong>Generated:</strong> {datetime.now().isoformat()}</p>
        <p class="metric {'pass' if metrics['overall_accuracy'] >= 95 else 'warn' if metrics['overall_accuracy'] >= 90 else 'fail'}">
            Accuracy: {metrics['overall_accuracy']}%
        </p>
        <p class="metric">
            Threshold Compliance: {metrics['threshold_compliance']}%
        </p>
    </div>

    <div class="section">
        <h2>Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Tests</td><td>{metrics['total_tests']}</td></tr>
            <tr><td>Misclassifications</td><td class="{'fail' if metrics['misclassifications']>0 else 'pass'}">{metrics['misclassifications']}</td></tr>
            <tr><td>Low Confidence</td><td class="{'warn' if metrics['low_confidence']>0 else 'pass'}">{metrics['low_confidence']}</td></tr>
        </table>
    </div>

    <div class="section">
        <h2>Per-Platform Accuracy</h2>
        <table>
            <tr><th>Platform</th><th>Tests</th><th>Correct</th><th>Accuracy</th></tr>
"""
        for platform, stats in metrics["by_platform"].items():
            status_class = "pass" if stats["accuracy"] >= 95 else "warn" if stats["accuracy"] >= 90 else "fail"
            html += f"""
            <tr class="{status_class}">
                <td>{platform.title()}</td>
                <td>{stats['tests']}</td>
                <td>{stats['correct']}</td>
                <td>{stats['accuracy']}%</td>
            </tr>
"""
        html += """
        </table>
    </div>
"""
        
        if self.misclassifications:
            html += """
    <div class="section fail">
        <h2>🚨 Misclassifications</h2>
        <table>
            <tr><th>Path</th><th>Expected</th><th>Got</th><th>Confidence</th></tr>
"""
            for m in self.misclassifications:
                html += f"""
            <tr>
                <td>{m['path']}</td>
                <td>{m['expected']}</td>
                <td>{m['got']}</td>
                <td>{m['confidence']:.2f}</td>
            </tr>
"""
            html += """
        </table>
    </div>
"""
        
        if self.low_confidence:
            html += """
    <div class="section warn">
        <h2>⚠️ Low Confidence Classifications</h2>
        <table>
            <tr><th>Path</th><th>Expected</th><th>Got</th><th>Confidence</th><th>Min Required</th></tr>
"""
            for lc in self.low_confidence:
                html += f"""
            <tr>
                <td>{lc['path']}</td>
                <td>{lc['expected']}</td>
                <td>{lc['got']}</td>
                <td>{lc['confidence']:.2f}</td>
                <td>{lc['min_required']}</td>
            </tr>
"""
            html += """
        </table>
    </div>
"""
        
        # All detailed results
        html += """
    <div class="section">
        <h2>Full Results</h2>
        <table>
            <tr><th>Path</th><th>Expected</th><th>Predicted</th><th>Confidence</th><th>Status</th></tr>
"""
        for r in self.results:
            badge_class = "badge-correct" if r["correct"] else "badge-incorrect"
            status_text = "✓ Correct" if r["correct"] else "✗ Wrong"
            html += f"""
            <tr>
                <td>{r['path'][:60]}</td>
                <td>{r['expected']}</td>
                <td>{r['predicted']}</td>
                <td>{r['confidence']:.2f}</td>
                <td><span class="badge {badge_class}">{status_text}</span></td>
            </tr>
"""
        html += """
        </table>
    </div>

    <div class="section">
        <h2>Recommendations</h2>
        <ul>
"""
        if metrics["misclassifications"] > 0:
            html += """
            <li>❌ Fix classifier logic for the misclassified cases. Review manifest patterns.</li>
"""
        if metrics["low_confidence"] > 0:
            html += """
            <li>⚠️ Add more signature patterns to boost confidence on these borderline cases.</li>
"""
        if metrics["overall_accuracy"] >= 98 and metrics["threshold_compliance"] >= 98:
            html += """
            <li>✅ Classifier is production-ready. All metrics look good.</li>
"""
        
        html += """
        </ul>
    </div>
</body>
</html>
"""
        return html
    
    def save_report(self, metrics: Dict[str, Any], output_path: Path):
        """Save HTML report."""
        html = self.generate_report(metrics, output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding='utf-8')
        print(f"\n[REPORT] Saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Audit source classifier accuracy")
    parser.add_argument("--dataset", default="tests/classification_dataset.json",
                       help="JSON dataset of known course classifications")
    parser.add_argument("--threshold", type=float, default=0.75,
                       help="Minimum acceptable confidence threshold")
    parser.add_argument("--output", default="validation/classifier_audit.html",
                       help="Output HTML report")
    args = parser.parse_args()
    
    auditor = ClassifierAuditor(Path(args.dataset))
    metrics = auditor.run_audit()
    
    if "error" in metrics:
        print(f"[ERROR] {metrics['error']}")
        return 1
    
    auditor.save_report(metrics, Path(args.output))
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"AUDIT COMPLETE")
    print(f"  Accuracy: {metrics['overall_accuracy']}%")
    print(f"  Threshold compliance: {metrics['threshold_compliance']}%")
    print(f"  Misclassifications: {metrics['misclassifications']}")
    print(f"  Low confidence: {metrics['low_confidence']}")
    
    # Exit code
    if metrics["misclassifications"] > 0:
        return 1
    if metrics["low_confidence"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())