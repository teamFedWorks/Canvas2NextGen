"""
Production Canvas -> Tutor LMS Migration Pipeline \ NextGen LMS

This is the main entry point for the migration pipeline.
Orchestrates all 5 stages of the migration process.
"""

import sys
import time
from pathlib import Path
from typing import Optional

from src.models.migration_report import MigrationReport, ReportStatus
from src.stages.validator import Validator
from src.stages.parser import Parser
from src.transformers.course_transformer import CourseTransformer
from src.exporters.tutor_exporter import TutorExporter
from src.exporters.report_generator import ReportGenerator


class MigrationPipeline:
    """
    Main migration pipeline orchestrator.
    
    Executes all 5 stages:
    1. Validation & Inventory
    2. Semantic Parsing
    3. Content Resolution (integrated into transformer)
    4. Tutor LMS Transformation
    5. Export & Verification
    """
    
    def __init__(self, course_directory: Path, output_directory: Optional[Path] = None, on_progress=None):
        """
        Initialize the migration pipeline.
        
        This setup phase identifies where the source files are and where the 
        converted output should be saved.
        
        Args:
            course_directory: Path to the unzipped Canvas course export directory.
            output_directory: Optional path for the converted output. Defaults to 'lms_output'.
            on_progress: Optional callback function(step_name, progress_pct, message)
        """
        # Convert path strings to Path objects for robust file handling
        self.course_directory = Path(course_directory)
        self.on_progress = on_progress
        
        # Set the output directory; default to 'lms_output' inside the course directory if not specified
        if output_directory is None:
            self.output_directory = self.course_directory / "lms_output"
        else:
            self.output_directory = Path(output_directory)
        
        # Ensure the output directory exists on the filesystem
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize the migration report which tracks success, errors, and metadata
        self.report = MigrationReport(
            status=ReportStatus.SUCCESS,
            source_directory=str(self.course_directory),
            output_directory=str(self.output_directory)
        )
    
    def run(self) -> MigrationReport:
        """
        Execute the complete 5-stage migration process.
        
        This is the main 'engine' of the script that runs validation, parsing,
        transformation, and export in a sequential pipeline.
        
        Returns:
            A MigrationReport object containing all details of the conversion.
        """
        start_time = time.time()
        
        print("=" * 80)
        print("CANVAS -> TUTOR LMS MIGRATION PIPELINE \ NextGen LMS v2.0")
        print("=" * 80)
        print()
        
        try:
            # Stage 1: Validation & Inventory
            # Before we start, we ensure the Canvas export folder has all required files (like imsmanifest.xml).
            print("[1/5] Validating Canvas export structure...")
            if self.on_progress: self.on_progress("validating", 10, "Validating Canvas export structure...")
            validation_report = self._stage_1_validate()
            self.report.validation_report = validation_report
            
            # If validation fails, we stop immediately to avoid processing corrupt data.
            if not validation_report.passed:
                print("[FAIL] Validation failed. See report for details.")
                if self.on_progress: self.on_progress("validating", 10, "Validation failed.")
                self.report.status = ReportStatus.FAILURE
                return self._finalize_report(start_time)
            
            print(f"[DONE] Validation passed")
            print(f"  - Found {validation_report.inventory.pages} pages")
            print(f"  - Found {validation_report.inventory.modules} modules")
            print()
            
            # Stage 2: Semantic Parsing
            # We read the XML files and convert them into our internal Python models.
            print("[2/5] Parsing Canvas content...")
            if self.on_progress: self.on_progress("parsing", 30, "Parsing Canvas content...")
            canvas_course, parse_report = self._stage_2_parse()
            self.report.parse_report = parse_report
            
            # If we couldn't parse the course structure, we shouldn't continue.
            if canvas_course is None:
                print("[FAIL] Parsing failed. See report for details.")
                if self.on_progress: self.on_progress("parsing", 30, "Parsing failed.")
                self.report.status = ReportStatus.FAILURE
                return self._finalize_report(start_time)
            
            # Store metadata about the original course for the report.
            self.report.source_course_title = canvas_course.title
            self.report.source_content_counts = canvas_course.get_content_counts()
            
            print(f"[DONE] Parsed course: {canvas_course.title}")
            print(f"  - Modules: {len(canvas_course.modules)}")
            print(f"  - Pages: {len(canvas_course.pages)}")
            print(f"  - Assignments: {len(canvas_course.assignments)}")
            print(f"  - Quizzes: {len(canvas_course.quizzes)}")
            print()
            
            # Stage 3 & 4: Transformation (includes content resolution)
            # This is where we map Canvas-specific fields to Tutor LMS-compatible fields.
            # It also handles rewriting internal links and fixing asset paths.
            print("[3/5] Transforming to Tutor LMS format...")
            if self.on_progress: self.on_progress("transforming", 60, "Transforming to Tutor LMS format...")
            tutor_course, transformation_report = self._stage_4_transform(canvas_course)
            self.report.transformation_report = transformation_report
            
            # Track counts of what was successfully transformed.
            self.report.migrated_content_counts = tutor_course.get_content_counts()
            
            print(f"[DONE] Transformed to Tutor LMS")
            print(f"  - Topics: {len(tutor_course.topics)}")
            print(f"  - Lessons: {transformation_report.lessons_created}")
            print(f"  - Assignments: {transformation_report.assignments_created}")
            print(f"  - Quizzes: {transformation_report.quizzes_created}")
            print(f"  - Questions: {transformation_report.questions_created}")
            print()
            
            # Stage 5: Export & Verification
            # Finally, we save the memory-resident models into physical JSON and HTML files.
            print("[4/5] Exporting to JSON...")
            if self.on_progress: self.on_progress("exporting", 80, "Exporting to JSON...")
            verification_report = self._stage_5_export(tutor_course)
            self.report.verification_report = verification_report
            
            print(f"[DONE] Exported to {self.output_directory}")
            print(f"  - Output size: {verification_report.total_output_size_mb:.2f} MB")
            print()
            
            # Generate human-readable reports for the user.
            print("[5/5] Generating migration reports...")
            if self.on_progress: self.on_progress("finalizing", 95, "Generating migration reports...")
            self._generate_reports()
            
            print("[DONE] Reports generated")
            print()
            
        except Exception as e:
            print(f"[FAIL] Unexpected error: {str(e)}")
            self.report.status = ReportStatus.FAILURE
            import traceback
            traceback.print_exc()
        
        return self._finalize_report(start_time)
    
    def _stage_1_validate(self):
        """Stage 1: Validation & Inventory - Checks if the input folder structure is correct."""
        validator = Validator(self.course_directory)
        return validator.validate()
    
    def _stage_2_parse(self):
        """Stage 2: Semantic Parsing - Reads Canvas files and converts them to Python objects."""
        parser = Parser(self.course_directory)
        return parser.parse()
    
    def _stage_4_transform(self, canvas_course):
        """Stage 4: Tutor LMS Transformation - Adapts Canvas data to the Tutor LMS schema."""
        transformer = CourseTransformer()
        return transformer.transform(canvas_course)
    
    def _stage_5_export(self, tutor_course):
        """Stage 5: Export & Verification - Saves output files to the lms_output directory."""
        exporter = TutorExporter(self.output_directory, self.course_directory)
        return exporter.export(tutor_course)
    
    def _generate_reports(self):
        """Generates the JSON and HTML migration reports for review."""
        report_generator = ReportGenerator(self.output_directory)
        report_generator.generate(self.report)
    
    def _finalize_report(self, start_time: float) -> MigrationReport:
        """Calculates final metrics and prints the closing summary banner."""
        self.report.execution_time_seconds = time.time() - start_time
        self.report.aggregate_errors()
        
        print("=" * 80)
        print("MIGRATION COMPLETE")
        print("=" * 80)
        print(f"Status: {self.report.status.value.upper()}")
        print(f"Errors: {self.report.total_errors}")
        print(f"Warnings: {self.report.total_warnings}")
        print(f"Execution time: {self.report.execution_time_seconds:.2f}s")
        print()
        print(f"Output directory: {self.output_directory}")
        print(f"  - tutor_course.json")
        print(f"  - migration_report.json")
        print(f"  - migration_report.html")
        print(f"  - IMPORT_INSTRUCTIONS.md")
        print("=" * 80)
        
        return self.report


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Production Canvas → Tutor LMS Migration Pipeline \ NextGen LMS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python Canvas_Converter.py ./cs-1143
  python Canvas_Converter.py ./cs-1143 ./output
  python Canvas_Converter.py ./cs-1143 --upload
  python Canvas_Converter.py ./cs-1143 --upload --env-file .env.production
        """
    )
    
    parser.add_argument(
        'course_directory',
        type=str,
        help='Path to Canvas course export directory'
    )
    
    parser.add_argument(
        'output_directory',
        type=str,
        nargs='?',
        default=None,
        help='Optional output directory (default: course_dir/lms_output)'
    )
    
    parser.add_argument(
        '--upload',
        action='store_true',
        help='Upload to MongoDB after conversion'
    )
    
    parser.add_argument(
        '--env-file',
        type=str,
        default='.env',
        help='Path to .env file for MongoDB configuration (default: .env)'
    )
    
    args = parser.parse_args()
    
    # Run conversion pipeline
    pipeline = MigrationPipeline(args.course_directory, args.output_directory)
    report = pipeline.run()
    
    # Exit with error code if migration failed
    if report.status == ReportStatus.FAILURE:
        sys.exit(1)
    
    # Upload to MongoDB if requested
    if args.upload:
        print()
        print("=" * 80)
        print("UPLOADING TO MONGODB")
        print("=" * 80)
        print()
        
        try:
            from pathlib import Path
            from src.config.mongodb_config import MongoDBConfig
            from src.exporters.mongodb_uploader import MongoDBUploader
            
            # Determine course JSON path
            if args.output_directory:
                output_dir = Path(args.output_directory)
            else:
                output_dir = Path(args.course_directory) / "lms_output"
            
            course_json_path = output_dir / "course.json"
            
            if not course_json_path.exists():
                print(f"[FAIL] Error: Course JSON not found at {course_json_path}")
                sys.exit(1)
            
            # Load MongoDB configuration
            env_file = Path(args.env_file) if Path(args.env_file).exists() else None
            config = MongoDBConfig(env_file)
            
            # Create uploader and upload
            uploader = MongoDBUploader(config)
            
            if not uploader.connect():
                print("[FAIL] Failed to connect to MongoDB")
                sys.exit(1)
            uploader.disconnect()
            
            if success:
                print()
                print("=" * 80)
                print("CONVERSION AND UPLOAD COMPLETE")
                print("=" * 80)
            else:
                print("[FAIL] Upload failed")
                sys.exit(1)
                
        except ImportError:
            print("[FAIL] MongoDB dependencies not installed.")
            print("   Install with: pip install pymongo python-dotenv")
            sys.exit(1)
        except Exception as e:
            print(f"[FAIL] Upload error: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
