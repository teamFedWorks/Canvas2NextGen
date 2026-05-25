Set-StrictMode -Version Latest
$ErrorActionPreference = 'SilentlyContinue'

$ECR_REPO = "lms/content-ingestion"
$REGION    = "us-east-2"
$ACCOUNT_ID = "129617679313"
$ECR_REGISTRY = "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# Simple approach – capture stderr into a variable
$errOut = & aws ecr create-repository --repository-name $ECR_REPO --region $REGION --image-scanning-configuration scanOnPush=true --tags Key=Environment,Value=staging Key=Service,Value=content-ingestion --output text 2>&1 | Out-Null
Write-Host "LASTEXITCODE: $LASTEXITCODE"
Write-Host "errOut is null: $($null -eq $errOut)"
Write-Host "errOut: [$errOut]"
Write-Host "errOut type: $($errOut.GetType().Name)"

# Try another way
$ecrOut2 = & aws ecr create-repository --repository-name $ECR_REPO --region $REGION --image-scanning-configuration scanOnPush=true --tags Key=Environment,Value=staging Key=Service,Value=content-ingestion --output text 2>&1 2> stub_err | Out-Null
Write-Host "LASTEXITCODE2: $LASTEXITCODE2 if exists"

# The ErrorRecord way
$Error.Clear()
$ErrorActionPreference = 'Continue'
$ecrOut3 = & aws ecr create-repository --repository-name $ECR_REPO --region $REGION --image-scanning-configuration scanOnPush=true --tags Key=Environment,Value=staging Key=Service,Value=content-ingestion --output text 2>&1 | Out-Null
$ecrOut3 | Out-Null 2>$null
Write-Host "LASTEXITCODE3: $LASTEXITCODE"
for ($i = 0; $i -lt [math]::Min(2, $Error.Count); $i++) { Write-Host "ERROR[$i]: $($Error[$i].Exception.Message)" }
