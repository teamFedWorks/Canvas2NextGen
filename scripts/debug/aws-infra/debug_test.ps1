Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$global:LASTEXITCODE = 0
$ErrorActionPreference = 'SilentlyContinue'
$out = & aws ecr describe-repositories --repository-names lms/content-ingestion --region us-east-2 --query repositories[0].repositoryUri --output text 2>$null | Out-Null
$ErrorActionPreference = 'Stop'
Write-Host "OUTPUT: $out"
Write-Host "LASTEXITCODE: $LASTEXITCODE"
