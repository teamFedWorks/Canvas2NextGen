$BucketName = "eduvatehub-courseshells-prod"

$Paths = @(
    "Institutions/",
    "Institutions/SFC/",
    "Institutions/SFC/programs/",
    "Institutions/SFC/programs/bs-computer-science/",
    "Institutions/SFC/programs/bs-computer-science/courses/",
    "Institutions/SFC/programs/bs-information-technology/",
    "Institutions/SFC/programs/bs-information-technology/courses/"
)

foreach ($Path in $Paths) {
    Write-Host "Creating folder: $Path"
    aws s3api put-object --bucket $BucketName --key $Path
}

Write-Host "S3 Folder Structure Initialized Successfully."
