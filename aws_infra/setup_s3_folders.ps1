$BucketName = "eduvatehub-courseshells-prod"

$Paths = @(
    "Institutions/",
    "Institutions/SFC-university/",
    "Institutions/SFC-university/programs/",
    "Institutions/SFC-university/programs/bs-computer-science/",
    "Institutions/SFC-university/programs/bs-computer-science/courses/",
    "Institutions/SFC-university/programs/bs-information-technology/",
    "Institutions/SFC-university/programs/bs-information-technology/courses/"
)

foreach ($Path in $Paths) {
    Write-Host "Creating folder: $Path"
    aws s3api put-object --bucket $BucketName --key $Path
}

Write-Host "S3 Folder Structure Initialized Successfully."
