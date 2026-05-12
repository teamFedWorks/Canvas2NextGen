import zipfile
import os
import shutil
from pathlib import Path
from utils.zip_utils import safe_extractall

upload_dir = Path(r"B:\EduvateHub\CourseOnboarding\storage\uploads")
temp_extract = upload_dir / "temp_standardize"

def standardize_course(course_code: str, search_pattern: str):
    print(f"Standardizing {course_code}...")
    # Find any zip in the specific folder
    course_folder = next(upload_dir.glob(f"*{course_code}*"), None)
    if not course_folder or not course_folder.is_dir():
        print(f"  Skipping {course_code}: folder not found.")
        return

    inner_zip = next(course_folder.glob("*.zip"), None)
    if not inner_zip:
        print(f"  Skipping {course_code}: inner zip not found.")
        return

    # Extract inner zip to temp
    if temp_extract.exists():
        shutil.rmtree(temp_extract)
    temp_extract.mkdir()
    
    with zipfile.ZipFile(inner_zip, 'r') as z:
        safe_extractall(z, temp_extract)
    
    # Check if there's a manifest
    if not (temp_extract / "imsmanifest.xml").exists():
        # Maybe it's nested AGAIN?
        # Check if there's a single folder inside
        content = list(temp_extract.iterdir())
        if len(content) == 1 and content[0].is_dir():
            print(f"  Found extra nesting in {course_code}, shifting up...")
            for item in content[0].iterdir():
                shutil.move(str(item), str(temp_extract))
            shutil.rmtree(content[0])

    # Re-zip to the top level
    target_zip = upload_dir / f"{course_code.lower()}_fixed.zip"
    with zipfile.ZipFile(target_zip, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(temp_extract):
            for file in files:
                filepath = Path(root) / file
                arcname = filepath.relative_to(temp_extract)
                z.write(filepath, arcname)
    
    print(f"  Created standardized package: {target_zip.name}")

# Standardize all 10 courses
standardize_course("ENT-1001", "ENT-1001")
standardize_course("ENT-1777", "ENT-1777")
standardize_course("IT-1104", "IT-1104")
standardize_course("IT-2105", "IT-2105")
standardize_course("IT-2510", "IT-2510")
standardize_course("IT-2620", "IT-2620")
standardize_course("IT-3101", "IT-3101")
standardize_course("IT-3301", "IT-3301")
standardize_course("IT-3310", "IT-3310")
standardize_course("IT-4016", "IT-4016")

# IT-2620 special case: if it exists as a zip in root, just copy it to _fixed
if (upload_dir / "it-2620.zip").exists() and not (upload_dir / "it-2620_fixed.zip").exists():
    shutil.copy(upload_dir / "it-2620.zip", upload_dir / "it-2620_fixed.zip")

print("Standardization complete.")
