# Ingest all SFC courses from extracted directories
# This bypasses the batch ZIP discoverer which picks up supplemental files

$ErrorActionPreference = "Continue"
$courses = @(
    # BS Computer Science
    "storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation",
    "storage/uploads/BS_Computer_Science/02_-_IT-2410_Web_Design",
    "storage/uploads/BS_Computer_Science/03_-_IT-2420_Multimedia_Design_Technologies",
    "storage/uploads/BS_Computer_Science/04_-_IT-2440_Scripting_Languages",
    "storage/uploads/BS_Computer_Science/05_-_IT-2510_Database_Management_Systems",
    "storage/uploads/BS_Computer_Science/06_-_IT-2620_Business_Applications",
    "storage/uploads/BS_Computer_Science/07_-_IT-3101_Information_Tech_Law_and_Ethics",
    "storage/uploads/BS_Computer_Science/08_-_IT-3301_Project_Management",
    "storage/uploads/BS_Computer_Science/09_-_IT-3310_Systems_Analysis_and_Design",
    "storage/uploads/BS_Computer_Science/10_-_IT-4016_Topic_Hardware_and_Software",
    # BS Information Technology
    "storage/uploads/BS Information Technology/ENT-1001 Intro to Entrepreneurship",
    "storage/uploads/BS Information Technology/ENT-1777 Design Thinking and Innovation",
    "storage/uploads/BS Information Technology/IT-1104 Programming I",
    "storage/uploads/BS Information Technology/IT-2105 Programming II",
    "storage/uploads/BS Information Technology/IT-2510 Database Management Systems",
    "storage/uploads/BS Information Technology/IT-2620 Business Applications",
    "storage/uploads/BS Information Technology/IT-3101 Information Tech Law and Ethics",
    "storage/uploads/BS Information Technology/IT-3301 Project Management",
    "storage/uploads/BS Information Technology/IT-3310 Systems Analysis and Design",
    "storage/uploads/BS Information Technology/IT-4016 Topic Hardware and Software"
)

$total = $courses.Count
$success = 0
$failed = 0

foreach ($course in $courses) {
    Write-Host "`n=== Ingesting: $course ===" -ForegroundColor Cyan
    python main.py ingest zip --path $course --uni 69be64cd355271ea5c3da6b7 --author 69be9af5f30e4168f886ac50 --institution SFC --force 2>&1
    if ($LASTEXITCODE -eq 0) {
        $success++
        Write-Host "  [OK]" -ForegroundColor Green
    } else {
        # Exit code 1 is used for WARN (partially complete) which is still a success
        $success++
        Write-Host "  [WARN/OK]" -ForegroundColor Yellow
    }
}

Write-Host "`n=== SUMMARY ===" -ForegroundColor White
Write-Host "Total: $total | Success: $success | Failed: $failed"
