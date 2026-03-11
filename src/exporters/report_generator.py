"""
Report Generator - Creates comprehensive migration reports.

Generates both JSON and HTML migration reports.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from ..models.migration_report import MigrationReport, ErrorSeverity
from ..utils.file_utils import ensure_directory_exists


class ReportGenerator:
    """
    Generates migration reports in JSON and HTML formats.
    """
    
    def __init__(self, output_directory: Path):
        """
        Initialize the Report Generator.
        
        Args:
            output_directory: Where the final reports (JSON/HTML) will be saved.
        """
        self.output_directory = output_directory
        # Ensure the output directory exists so we can write the reports.
        ensure_directory_exists(output_directory)
    
    def generate(self, report: MigrationReport) -> None:
        """
        Produce both the machine-readable (JSON) and human-readable (HTML) reports.
        
        Args:
            report: The master MigrationReport object containing all stage results.
        """
        # Step 1: Collect errors/warnings from all 5 pipeline stages into one list.
        report.aggregate_errors()
        
        # Step 2: Create the .json file (ideal for automated auditing).
        self._generate_json_report(report)
        
        # Step 3: Create the .html file (ideal for humans to review).
        self._generate_html_report(report)
    
    def _generate_json_report(self, report: MigrationReport) -> None:
        """
        Saves the migration metadata into a structured JSON file.
        """
        # We build a custom dictionary from the MigrationReport object.
        report_dict = {
            "summary": report.get_summary_dict(),
            "source_content": report.source_content_counts,
            "migrated_content": report.migrated_content_counts,
            "errors": [
                {
                    "severity": error.severity.value,
                    "type": error.error_type,
                    "message": error.message,
                    "file_path": error.file_path,
                    "line_number": error.line_number,
                    "suggested_action": error.suggested_action,
                    "timestamp": error.timestamp.isoformat()
                }
                for error in report.all_errors
            ],
            "validation": {
                "passed": report.validation_report.passed if report.validation_report else False,
                "missing_files": report.validation_report.missing_file_list if report.validation_report else []
            } if report.validation_report else None,
            "transformation": {
                "question_type_mappings": report.transformation_report.question_type_mappings if report.transformation_report else {}
            } if report.transformation_report else None
        }
        
        # Write the data to migration_report.json
        output_file = self.output_directory / "migration_report.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)
    
    def _generate_html_report(self, report: MigrationReport) -> None:
        """Generate HTML migration report"""
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Canvas to Tutor LMS Migration Report \ NextGen LMS</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .status {{ display: inline-block; padding: 5px 15px; border-radius: 4px; font-weight: bold; }}
        .status.success {{ background: #4CAF50; color: white; }}
        .status.warning {{ background: #FF9800; color: white; }}
        .status.error {{ background: #f44336; color: white; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .summary-card {{ background: #f9f9f9; padding: 15px; border-radius: 4px; border-left: 4px solid #4CAF50; }}
        .summary-card h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
        .summary-card .value {{ font-size: 32px; font-weight: bold; color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #4CAF50; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .error-item {{ margin: 10px 0; padding: 10px; border-left: 4px solid #f44336; background: #fff3f3; }}
        .warning-item {{ margin: 10px 0; padding: 10px; border-left: 4px solid #FF9800; background: #fff8e1; }}
        .info-item {{ margin: 10px 0; padding: 10px; border-left: 4px solid #2196F3; background: #e3f2fd; }}
        .error-type {{ font-weight: bold; color: #f44336; }}
        .warning-type {{ font-weight: bold; color: #FF9800; }}
        .info-type {{ font-weight: bold; color: #2196F3; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-family: monospace; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Canvas → Tutor LMS Migration Report \ NextGen LMS</h1>
        
        <p><strong>Migration Date:</strong> {report.migration_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Status:</strong> <span class="status {self._get_status_class(report)}">{report.status.value.upper()}</span></p>
        <p><strong>Execution Time:</strong> {report.execution_time_seconds:.2f} seconds</p>
        
        <h2>Summary</h2>
        <div class="summary">
            <div class="summary-card">
                <h3>Lessons</h3>
                <div class="value">{report.migrated_content_counts.get('lessons', 0)}</div>
            </div>
            <div class="summary-card">
                <h3>Quizzes</h3>
                <div class="value">{report.migrated_content_counts.get('quizzes', 0)}</div>
            </div>
            <div class="summary-card">
                <h3>Assignments</h3>
                <div class="value">{report.migrated_content_counts.get('assignments', 0)}</div>
            </div>
            <div class="summary-card">
                <h3>Questions</h3>
                <div class="value">{report.migrated_content_counts.get('questions', 0)}</div>
            </div>
        </div>
        
        <h2>Content Migration</h2>
        <table>
            <tr>
                <th>Content Type</th>
                <th>Source (Canvas)</th>
                <th>Migrated (Tutor)</th>
            </tr>
            {self._generate_content_table_rows(report)}
        </table>
        
        <h2>Issues & Warnings</h2>
        <p><strong>Errors:</strong> {report.total_errors} | <strong>Warnings:</strong> {report.total_warnings} | <strong>Info:</strong> {report.total_info}</p>
        
        {self._generate_error_list(report)}
        
        <h2>Output Files</h2>
        <ul>
            <li><code>tutor_course.json</code> - Tutor LMS course structure</li>
            <li><code>migration_report.json</code> - Machine-readable report</li>
            <li><code>IMPORT_INSTRUCTIONS.md</code> - Import instructions</li>
        </ul>
        
        <hr>
        <p style="color: #999; font-size: 12px;">Generated by Canvas to Tutor LMS Converter \ NextGen LMS v2.0.0</p>
    </div>
</body>
</html>"""
        
        output_file = self.output_directory / "migration_report.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
    
    def _get_status_class(self, report: MigrationReport) -> str:
        """Get CSS class for status"""
        if report.status.value == 'success':
            return 'success'
        elif report.status.value in ('success_with_warnings', 'partial_failure'):
            return 'warning'
        else:
            return 'error'
    
    def _generate_content_table_rows(self, report: MigrationReport) -> str:
        """Generate table rows for content comparison"""
        rows = []
        # Mapping: label, source_key (Canvas), target_key (Tutor)
        entities = [
            ('Modules', 'modules', 'topics'),
            ('Pages', 'pages', 'lessons'),
            ('Assignments', 'assignments', 'assignments'),
            ('Quizzes', 'quizzes', 'quizzes'),
            ('Questions', 'questions', 'questions')
        ]
        
        for label, source_key, target_key in entities:
            source_count = report.source_content_counts.get(source_key, 0)
            migrated_count = report.migrated_content_counts.get(target_key, 0)
            rows.append(f"""
            <tr>
                <td>{label}</td>
                <td>{source_count}</td>
                <td>{migrated_count}</td>
            </tr>
            """)
        
        return ''.join(rows)
    
    def _generate_error_list(self, report: MigrationReport) -> str:
        """Generate HTML list of errors"""
        if not report.all_errors:
            return "<p>No issues found! ✓</p>"
        
        html_parts = []
        
        for error in report.all_errors:
            if error.severity == ErrorSeverity.CRITICAL or error.severity == ErrorSeverity.ERROR:
                css_class = "error-item"
                type_class = "error-type"
            elif error.severity == ErrorSeverity.WARNING:
                css_class = "warning-item"
                type_class = "warning-type"
            else:
                css_class = "info-item"
                type_class = "info-type"
            
            html_parts.append(f"""
            <div class="{css_class}">
                <div class="{type_class}">[{error.severity.value.upper()}] {error.error_type}</div>
                <p>{error.message}</p>
                {f'<p><strong>File:</strong> <code>{error.file_path}</code></p>' if error.file_path else ''}
                {f'<p><strong>Suggested Action:</strong> {error.suggested_action}</p>' if error.suggested_action else ''}
            </div>
            """)
        
        return ''.join(html_parts)
