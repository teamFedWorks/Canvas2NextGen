Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Replicate _Invoke-AWSRaw exactly as deployed
function _Invoke-AWSRaw-InScript {
    param([string[]]$AwsArgs)
    $global:LASTEXITCODE = 0
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'SilentlyContinue'
    try {
        & aws @AwsArgs 2>$null | Out-Null
    } catch {
    } finally {
        $ErrorActionPreference = $prev
    }
}

function Test-AWS-InScript {
    param([string[]]$AwsArgs)
    _Invoke-AWSRaw-InScript @AwsArgs
    Write-Host "LASTEXITCODE=$LASTEXITCODE"
    return ($LASTEXITCODE -eq 0)
}

$REGION = "us-east-2"
$ECR_REPO = "lms/content-ingestion"

$result = Test-AWS-InScript @("ecr", "describe-repositories",
    "--repository-names", $ECR_REPO,
    "--region", $REGION,
    "--query", "repositories[0].repositoryUri",
    "--output", "text")

Write-Host "result=$result"

# Also test JSON output
$result2 = Test-AWS-InScript @("ecr", "describe-repositories",
    "--repository-names", $ECR_REPO,
    "--region", $REGION,
    "--output", "json")

Write-Host "result2=$result2"
