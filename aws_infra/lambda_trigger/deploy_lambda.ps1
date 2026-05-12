#!/usr/bin/env powershell
# =============================================================================
# Deploy / Update: course-shell-upload-email-formatter Lambda
#
# What this script does (and ONLY this):
#   1. Zips the updated lambda_function.py
#   2. Uploads the new code to the existing Lambda function
#   3. Updates the Lambda environment variables to add the ingestion API config
#
# What it does NOT touch:
#   - The S3 bucket notification configuration (already set up by PM)
#   - The SNS topic or its subscriptions
#   - The Lambda's IAM role or resource policy
#   - Any other Lambda function
#   - Any ECS service, VPC, or other infrastructure
#
# Prerequisites:
#   - AWS CLI configured with credentials that have lambda:UpdateFunctionCode
#     and lambda:UpdateFunctionConfiguration permissions
#   - The ECS CourseOnboarding service must be running and its private IP known
#     (run .\aws_infra\start-service.ps1 to get the IP)
#
# Usage:
#   # Dry-run — show what would change, make no AWS calls
#   .\aws_infra\lambda_trigger\deploy_lambda.ps1 -DryRun
#
#   # Deploy with the API URL of the running ECS task
#   .\aws_infra\lambda_trigger\deploy_lambda.ps1 `
#       -ApiUrl "http://10.0.1.45:5009/api/v1" `
#       -ApiKey  "your-lms-api-key" `
#       -WbuUniversityId "mongo-object-id-for-wbu" `
#       -WbuAuthorId     "mongo-object-id-for-wbu-author"
#
#   # Also set SFC IDs if SFC courses should auto-ingest too
#   .\aws_infra\lambda_trigger\deploy_lambda.ps1 `
#       -ApiUrl "http://10.0.1.45:5009/api/v1" `
#       -ApiKey  "your-lms-api-key" `
#       -WbuUniversityId "..." -WbuAuthorId "..." `
#       -SfcUniversityId "..." -SfcAuthorId "..."
# =============================================================================

param(
    [string]$ApiUrl          = "",
    [string]$ApiKey          = "",
    [string]$WbuUniversityId = "",
    [string]$WbuAuthorId     = "",
    [string]$SfcUniversityId = "",
    [string]$SfcAuthorId     = "",
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$REGION    = "us-east-2"
$FUNC_NAME = "course-shell-upload-email-formatter"
$SCRIPT_DIR = Split-Path -Parent $PSCommandPath

function Log-Step { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Log-Ok   { param([string]$msg) Write-Host "    OK: $msg" -ForegroundColor Green }
function Log-Skip { param([string]$msg) Write-Host "    SKIP: $msg" -ForegroundColor Yellow }
function Log-Info { param([string]$msg) Write-Host "    $msg" -ForegroundColor Gray }
function Log-Warn { param([string]$msg) Write-Host "    WARN: $msg" -ForegroundColor Yellow }

# ── Pre-flight ────────────────────────────────────────────────────────────────
Log-Step "Pre-flight"

$identity = aws sts get-caller-identity --region $REGION --output json | ConvertFrom-Json
Log-Info "Account : $($identity.Account)"
Log-Info "User    : $($identity.Arn)"

# Verify the function exists
$funcInfo = aws lambda get-function-configuration `
    --function-name $FUNC_NAME `
    --region $REGION `
    --output json | ConvertFrom-Json
Log-Info "Function : $($funcInfo.FunctionName)"
Log-Info "Runtime  : $($funcInfo.Runtime)"
Log-Info "Handler  : $($funcInfo.Handler)"

if (-not $ApiUrl) {
    Log-Warn "No -ApiUrl provided.  The Lambda will send email alerts but NOT trigger ingestion."
    Log-Warn "To enable auto-ingestion, re-run with -ApiUrl pointing to the ECS task private IP."
}

# ── Step 1: Package the Lambda code ──────────────────────────────────────────
Log-Step "Step 1/2 - Package Lambda code"

$zipPath = Join-Path $env:TEMP "course_shell_lambda.zip"
$srcFile = Join-Path $SCRIPT_DIR "lambda_function.py"

if (-not (Test-Path $srcFile)) {
    throw "lambda_function.py not found at: $srcFile"
}

if ($DryRun) {
    Log-Skip "DryRun — would zip $srcFile → $zipPath"
} else {
    # Use .NET's ZipFile to avoid requiring 7-zip or Compress-Archive quirks
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
    $zip = [System.IO.Compression.ZipFile]::Open($zipPath, 'Create')
    [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
        $zip, $srcFile, "lambda_function.py",
        [System.IO.Compression.CompressionLevel]::Optimal
    ) | Out-Null
    $zip.Dispose()
    $zipSize = (Get-Item $zipPath).Length
    Log-Ok "Packaged lambda_function.py → $zipPath ($zipSize bytes)"
}

# ── Step 2: Update function code ──────────────────────────────────────────────
Log-Step "Step 2/2 - Update Lambda"

if ($DryRun) {
    Log-Skip "DryRun — would call lambda:UpdateFunctionCode"
    Log-Skip "DryRun — would call lambda:UpdateFunctionConfiguration"
} else {
    # 2a. Upload new code
    aws lambda update-function-code `
        --function-name $FUNC_NAME `
        --zip-file "fileb://$zipPath" `
        --region $REGION `
        --output text | Out-Null
    Log-Ok "Function code updated"

    # 2b. Build environment variables object
    # Always preserve the existing SNS vars; add/update the ingestion vars
    $existingEnv = $funcInfo.Environment.Variables
    $newEnv = @{
        SNS_TOPIC_ARN    = $existingEnv.SNS_TOPIC_ARN
        AWS_REGION_NAME  = $existingEnv.AWS_REGION_NAME
        ONBOARDING_API_URL = $ApiUrl
        ONBOARDING_API_KEY = $ApiKey
        WBU_UNIVERSITY_ID  = $WbuUniversityId
        WBU_AUTHOR_ID      = $WbuAuthorId
        SFC_UNIVERSITY_ID  = $SfcUniversityId
        SFC_AUTHOR_ID      = $SfcAuthorId
    }

    # Serialise to the AWS CLI format: "Variables={KEY=VAL,KEY2=VAL2}"
    $varPairs = ($newEnv.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ","
    $envString = "Variables={$varPairs}"

    aws lambda update-function-configuration `
        --function-name $FUNC_NAME `
        --environment $envString `
        --region $REGION `
        --output text | Out-Null
    Log-Ok "Environment variables updated"

    # Clean up temp zip
    Remove-Item $zipPath -Force
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Lambda Deployment Complete" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host " Function : $FUNC_NAME"
Write-Host " Region   : $REGION"
Write-Host ""

if ($ApiUrl) {
    Write-Host " Auto-ingestion : ENABLED" -ForegroundColor Green
    Write-Host "   API URL : $ApiUrl"
    Write-Host "   WBU IDs : university=$WbuUniversityId  author=$WbuAuthorId"
    if ($SfcUniversityId) {
        Write-Host "   SFC IDs : university=$SfcUniversityId  author=$SfcAuthorId"
    }
} else {
    Write-Host " Auto-ingestion : DISABLED (no API URL configured)" -ForegroundColor Yellow
    Write-Host "   Email alerts will still be sent." -ForegroundColor Yellow
    Write-Host "   Re-run with -ApiUrl to enable auto-ingestion." -ForegroundColor Yellow
}

Write-Host ""
Write-Host " To test the Lambda manually:" -ForegroundColor Gray
Write-Host "   aws lambda invoke --function-name $FUNC_NAME --region $REGION" -ForegroundColor Gray
Write-Host "   --payload file://aws_infra/lambda_trigger/test_event.json /tmp/out.json" -ForegroundColor Gray
Write-Host ""
Write-Host " To watch live logs:" -ForegroundColor Gray
Write-Host "   aws logs tail /aws/lambda/$FUNC_NAME --region $REGION --follow" -ForegroundColor Gray
Write-Host ""
