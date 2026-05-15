# Pipeline Validation Suite

Comprehensive validation toolkit for the canonical LMS ingestion pipeline.

## Overview

This validation suite ensures zero-data-loss migration from the legacy `MigrationPipeline` to the new `CanonicalPipeline`. It provides multi-layered verification:

```
┌──────────────────────────────────────────────────────────┐
│  VALIDATION LAYERS                                       │
├──────────────────────────────────────────────────────────┤
│  Layer 1: Golden Tests     - Known-correct regression    │
│  Layer 2: Reconciliation   - Legacy vs Canonical diff     │
│  Layer 3: Classifier Audit - LMS detection accuracy      │
│  Layer 4: Schema Integrity - BSON serialization          │
│  Layer 5: E2E Pipeline     - Full integration            │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

Run all checks in one command:

```bash
python scripts/validate_deployment_readiness.py --all
```

This produces a comprehensive HTML report at `validation/readiness_report.html`.

## Individual Checks

### 1. Golden Dataset Tests (`test_chunked_exporter.py`)

Tests against a curated set of courses with known correct outputs.

```bash
python scripts/test_chunked_exporter.py
```

**Validates:**
- Model serialization to BSON
- Document size calculations
- Chunked export structure

**Golden Dataset Location:** `tests/golden_dataset.json`

**Sample output:**
```
============================================================
 CHUNKED EXPORTER UNIT TESTS
============================================================
TEST: Exporter Initialization             [PASS]
TEST: Model Serialization                 [PASS]
TEST: Document Size Estimation             [PASS]
TEST: Chunked Export Structure             [PASS]
```

---

### 2. Pipeline Reconciliation (`reconcile_pipelines.py`)

Runs both legacy and canonical pipelines on the same input and compares outputs field-by-field.

```bash
# Single course
python scripts/reconcile_pipelines.py \
  --course-dirs storage/uploads/BS\ Information\ Technology/IT-1104\ Programming\ I \
  --output validation/reconciliation_report.html

# Multiple courses
python scripts/reconcile_pipelines.py \
  --course-dirs storage/uploads/BS\ Information\ Technology/IT-1104\ Programming\ I \
                storage/uploads/BS\ Information\ Technology/IT-2105\ Programming\ II \
  --output validation/batch_reconciliation.html
```

**Validates:**
- Content completeness (modules, lessons, quizzes, questions, assets)
- Asset coverage (every legacy asset has canonical equivalent)
- Question type distribution preservation
- Module structure alignment

**Reconciliation Score Calculation:**
```
Score = 0.4 * content_count_accuracy
       + 0.3 * asset_coverage_pct
       + 0.2 * question_distribution_match
       + 0.1 * module_structure_match
```

**Interpretation:**
- **≥ 99% (GREEN):** Pipelines in near-perfect agreement → Ready for production
- **95-98% (YELLOW):** Minor discrepancies → Review, acceptable for canary
- **< 95% (RED):** Significant differences → Fix before deployment

---

### 3. Classifier Audit (`audit_classifier.py`)

Measures source detection accuracy on known LMS exports.

```bash
python scripts/audit_classifier.py \
  --dataset tests/classification_dataset.json \
  --threshold 0.75 \
  --output validation/classifier_audit.html
```

**Validates:**
- Platform identification (Canvas vs Blackboard vs Moodle)
- Confidence score calibration
- Misclassification rates

**Metrics:**
- **Accuracy:** % of correctly classified courses
- **Threshold Compliance:** % meeting minimum confidence (default 0.75)
- **Per-Platform Accuracy:** Breakdown by LMS type

**Alert thresholds:**
- Accuracy < 95% → Review classifier logic
- Confidence < 0.75 → Add more signature patterns

---

### 4. Schema Integrity Check (built into `validate_deployment_readiness.py`)

Ensures all canonical models are BSON-serializable without validation errors.

```bash
python scripts/validate_deployment_readiness.py --check-schema
```

**Validates:**
- Enum types convert to strings
- Datetime objects serialize correctly
- No circular references
- All fields are MongoDB-compatible types

---

### 5. End-to-End Pipeline Test (`validate_canonical_e2e.py`)

Full integration test from ZIP → Canonical → Chunked Export.

```bash
python scripts/validate_canonical_e2e.py
```

**Validates:**
- Classification → Extraction → Resolution → Parsing → Export flow
- All parser sub-components (PPTX, QTI, discussions)
- Asset detection and attachment
- Orphaned content handling

---

## Combined Validation

Run the complete suite:

```bash
python scripts/validate_deployment_readiness.py --all
```

This executes:
1. ✅ Golden dataset regression test
2. ✅ Pipeline reconciliation (sample courses)
3. ✅ Classifier accuracy audit
4. ✅ Schema integrity validation
5. ✅ Code quality checks
6. ✅ E2E pipeline integration

**Output:**
- `validation/readiness_report.html` - Comprehensive HTML dashboard
- `validation/*.json` - Machine-readable results

**Exit codes:**
- `0` - All checks passed (≥ 80% score)
- `1` - Critical failures (fix before deploying)

---

## Continuous Integration

Add to `.github/workflows/validation.yml`:

```yaml
name: Pipeline Validation
on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: 3.11
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      
      - name: Run full validation suite
        run: |
          python scripts/validate_deployment_readiness.py --all
      
      - name: Upload report
        uses: actions/upload-artifact@v2
        with:
          name: validation-report
          path: validation/
```

---

## Production Rollout Gates

### Gate 1: Pre-deploy (CI)
```
✅ Golden tests: 100% pass
✅ Schema integrity: PASS
✅ Code quality: PASS
→ Merge to main
```

### Gate 2: Staging (Manual)
```
✅ Reconciliation score ≥ 99% on 10 staging courses
✅ Classifier accuracy ≥ 98%
✅ E2E latency ≤ 120% of legacy
→ Promote to production canary
```

### Gate 3: Canary (5% traffic)
```
✅ Success rate gap < 2%
✅ Missing content rate < 0.1%
✅ No critical errors in logs
→ Increase to 25% after 24h
```

### Gate 4: Full Rollout (100%)
```
✅ 48h canary clean
✅ Reconciliation score maintained ≥ 99%
✅ Monitoring alerts silent
→ Cut over to 100% canonical
```

---

## Troubleshooting

### High misclassification rate
```bash
# Review classifier decisions
python scripts/audit_classifier.py --output audit.html
# Add more signature patterns to core/classifier.py
```

### Asset count mismatch
```bash
# Detailed reconciliation
python scripts/reconcile_pipelines.py \
  --course-dirs <problem_course> \
  --verbose
# Check asset resolution logic in adapters/canonical_adapter.py
```

### BSON serialization errors
```bash
# Identify problematic field
python -c "
from models.canonical_models import CanonicalCourse
import bson
course = CanonicalCourse(...)
print(course.__dict__)
"
# Ensure all enums use .value, objects implement to_dict()
```

---

## Additional Tools

### Generate Classification Dataset
```bash
# Build labeled dataset from existing courses
python scripts/build_classification_dataset.py \
  --input storage/uploads/ \
  --output tests/classification_dataset.json
```

### Stress Test Large Courses
```bash
# Test with largest courses in dataset
python scripts/stress_test.py \
  --find-largest storage/uploads/ --top 5 \
  --run-pipeline canonical
```

### Diff Course Content
```bash
# Show content-level differences between pipelines
python scripts/content_diff.py \
  --legacy-db course_id_123 \
  --canonical-db course_id_456 \
  --html diff_report.html
```

---

## Maintenance

**Weekly:**
- Run full validation on latest production uploads
- Review any new misclassifications

**Monthly:**
- Update golden dataset with new course patterns
- Re-calibrate classifier thresholds

**Per Release:**
- Run complete validation suite in CI
- Verify golden tests still pass
- Update this README with new checks

---

## Support

 Issues: `github.com/your-org/repo/issues`
 Docs: `kilo.ai/docs` (if using Kilo ecosystem)

**Remember:** Never deploy canonical pipeline without ≥ 99% reconciliation score on validated golden set.