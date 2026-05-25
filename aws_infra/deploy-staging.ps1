#!/usr/bin/env powershell
# =============================================================================
# Content Ingestion Service - AWS Staging Deployment Script
# Deploys the Content Ingestion Service to the staging ECS cluster with the
# service name: Content-Ingestion-staging-service
#
# Cluster : nextgen-lms-ecs-staging
# Service : Content-Ingestion-staging-service
#
# What this script creates (and ONLY this):
#   1. ECR repository          - lms/content-ingestion
#   2. Secrets Manager entries - /lms/staging/content-ingestion/*
#   3. IAM Task Role           - content-ingestion-task-role-staging
#   4. CloudWatch Log Group    - /ecs/course-onboarding-staging  (reused)
#   5. Security Group          - Content-Ingestion-sg-staging (port 5009)
#   6. ECS Task Definition     - content-ingestion-task-staging
#   7. ECS Fargate Service     - Content-Ingestion-staging-service
#      (on existing cluster: nextgen-lms-ecs-staging)
#
# Existing resources reused (read-only, never modified):
#   - VPC:              vpc-0e4dc54acdfbaa5e0
#   - Private Subnets:  subnet-0bb1286614658629a / subnet-0ac2defb986f4966e
#   - ECS Cluster:      nextgen-lms-ecs-staging
#   - Execution Role:   arn:aws:iam::129617679313:role/ecsTaskExecutionRole
#   - Task Role:        arn:aws:iam::129617679313:role/nextgen-lms-ecs-task-role
#   - S3 Bucket:        eduvatehub-courseshells-prod
#   - Log Group:        /ecs/course-onboarding-staging
#   - Staging Secrets:  /lms/staging/course-onboarding/*  (MONGODB_URI, LMS_API_KEY, S3_CDN_BASE_URL)
#
# Usage:
#   .\deploy-staging.ps1
#   .\deploy-staging.ps1 -DryRun          # validate only, no AWS calls
#   .\deploy-staging.ps1 -SkipBuild       # skip docker build+push
#   .\deploy-staging.ps1 -SkipSecrets     # secrets already created
# =============================================================================

param(
    [switch]$DryRun,
    [switch]$SkipBuild,
    [switch]$SkipSecrets
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
# _Invoke-AWSRaw runs the AWS CLI via `& aws @AwsArgs` absorbing PowerShell 5's
# native-command stderr.  $ErrorActionPreference='SilentlyContinue' handles the
# noisy stderr records.  ProcessStateInput .NET trick uses `StartInfo.Args`.
# The static argument format is handled per-call-site to avoid any char-escaping
# issues.  For args that include spaces as values (--tags), we write those values
# to a temp JSON file and substitute a --cli-input-json file://... reference.
function _Invoke-AWSRaw {
    param([string[]]$AwsArgs)
    $global:LASTEXITCODE = 0
    & aws @AwsArgs 2>$null | Out-Null
}

# Test-AWS: returns True if the AWS CLI call exits 0, False otherwise.
# Stderr is always silently absorbed via _Invoke-AWSRaw.
function Test-AWS {
    param([string[]]$AwsArgs)
    _Invoke-AWSRaw @AwsArgs
    return ($LASTEXITCODE -eq 0)
}

# --- CONFIG ------------------------------------------------------------------
$REGION          = "us-east-2"
$ACCOUNT_ID      = "129617679313"
$ECR_REGISTRY    = "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
$ECR_REPO        = "lms/content-ingestion"
$IMAGE_TAG       = "latest"
$IMAGE_URI       = "$ECR_REGISTRY/${ECR_REPO}:$IMAGE_TAG"

# Existing resources - DO NOT CHANGE
$VPC_ID          = "vpc-0e4dc54acdfbaa5e0"
$SUBNET_1        = "subnet-0bb1286614658629a"
$SUBNET_2        = "subnet-0ac2defb986f4966e"
$ECS_CLUSTER     = "nextgen-lms-ecs-staging"
$EXEC_ROLE_ARN   = "arn:aws:iam::${ACCOUNT_ID}:role/ecsTaskExecutionRole"
$TASK_ROLE_ARN   = "arn:aws:iam::${ACCOUNT_ID}:role/nextgen-lms-ecs-task-role"
$S3_BUCKET       = "eduvatehub-courseshells-prod"
$LOG_GROUP       = "/ecs/course-onboarding-staging"

# New / renamed resources for this deployment
$SG_NAME         = "Content-Ingestion-sg-staging"
$TASK_DEF_FAMILY = "content-ingestion-task-staging"
$SERVICE_NAME    = "Content-Ingestion-staging-service"
$SECRET_PREFIX   = "/lms/staging/content-ingestion"

# --- HELPERS -----------------------------------------------------------------
function Log-Step { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Log-Ok   { param([string]$msg) Write-Host "    OK: $msg" -ForegroundColor Green }
function Log-Skip { param([string]$msg) Write-Host "    SKIP: $msg" -ForegroundColor Yellow }
function Log-Info { param([string]$msg) Write-Host "    $msg" -ForegroundColor Gray }

# Invoke AWS CLI in a subprocess whose stderr/sdout we fully capture.  This is
# 100% immune to PowerShell 5's noisy native-command stderr (which would otherwise
# surface as a fatal PS error record even with SilentlyContinue).
function _Invoke-AWSRaw {
    param([string[]]$AwsArgs)
    $global:LASTEXITCODE = 0

    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'SilentlyContinue'
    try {
        & aws @AwsArgs 2>$null | Out-Null
    } catch {
        # swallow — exit code is checked by the caller
    } finally {
        $ErrorActionPreference = $prev
    }
}

# Invoke-AWSCapture: like Invoke-AWS but returns stdout as a string.
# Use this when you need to parse the output (e.g. JSON responses).
function Invoke-AWSCapture {
    param([string[]]$AwsArgs)
    if ($DryRun) {
        Write-Host "    [DRY-RUN] aws $($AwsArgs -join ' ')" -ForegroundColor DarkGray
        return $null
    }
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'SilentlyContinue'
    $output = $null
    try {
        $output = (& aws @AwsArgs 2>$null)
    } catch { }
    finally { $ErrorActionPreference = $prev }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    ERROR: AWS CLI exited with code $LASTEXITCODE" -ForegroundColor Red
        throw "AWS CLI command failed: aws $($AwsArgs -join ' ')"
    }
    return ($output -join "`n")
}

function Invoke-AWS {
    param([string[]]$AwsArgs)
    if ($DryRun) {
        Write-Host "    [DRY-RUN] aws $($AwsArgs -join ' ')" -ForegroundColor DarkGray
        return $null
    }
    _Invoke-AWSRaw @AwsArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    ERROR: AWS CLI exited with code $LASTEXITCODE" -ForegroundColor Red
        throw "AWS CLI command failed: aws $($AwsArgs -join ' ')"
    }
    return $null
}

# --- ENV EXISTENCE CHECK ------------------------------------------------------
Log-Step "Pre-flight checks"

if (-not (Test-Path "$PSScriptRoot/../.env") -and -not $SkipSecrets) {
    Log-Info "WARNING: No .env file found at $PSScriptRoot/../.env"
    Log-Info "         Secrets will be created as placeholders - remember to fill them in."
}

# --- PRE-FLIGHT --------------------------------------------------------------
$identity = aws sts get-caller-identity --region $REGION --output json | ConvertFrom-Json
Log-Info "Account : $($identity.Account)"
Log-Info "User    : $($identity.Arn)"

if ($identity.Account -ne $ACCOUNT_ID) {
    throw "Wrong AWS account! Expected $ACCOUNT_ID, got $($identity.Account)"
}
Log-Ok "Authenticated to correct account"

$clusterCheck = aws ecs describe-clusters --clusters $ECS_CLUSTER --region $REGION `
    --query "clusters[0].status" --output text
if ($clusterCheck -ne "ACTIVE") {
    throw "ECS cluster '$ECS_CLUSTER' not found or not ACTIVE"
}
Log-Ok "ECS cluster '$ECS_CLUSTER' is ACTIVE"

# --- STEP 1: ECR REPOSITORY --------------------------------------------------
Log-Step "Step 1/6 - ECR Repository"

# Try to create; if it already exists, log-and-skip.
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'SilentlyContinue'
$raw = & aws ecr create-repository --repository-name $ECR_REPO --region $REGION `
    --image-scanning-configuration scanOnPush=true `
    --tags Key=Environment,Value=staging Key=Service,Value=content-ingestion `
    --output text 2>&1 | Out-Null
$prevEAP = 'Stop'

if ($LASTEXITCODE -eq 0) {
    Log-Ok "Created ECR repo: $ECR_REGISTRY/$ECR_REPO"
} elseif ($LASTEXITCODE -ne 0) {
    # Repo already exists – this is fine, just skip
    Log-Skip "ECR repo '$ECR_REPO' already exists: $ECR_REGISTRY/$ECR_REPO"
} else {
    throw "ECR create-repository failed (exit $LASTEXITCODE)."
}

# --- STEP 2: DOCKER BUILD AND PUSH -------------------------------------------
Log-Step "Step 2/6 - Docker Build and Push"

if ($SkipBuild) {
    Log-Skip "SkipBuild flag set - assuming image already in ECR"
} elseif ($DryRun) {
    Log-Skip "DryRun - skipping docker build"
} else {
    # Script lives in CourseOnboarding/aws_infra/ so go up one level for docker context
    $dockerContext = Split-Path -Parent (Split-Path -Parent $PSCommandPath)

    Log-Info "Logging in to ECR..."
    $loginPwd = aws ecr get-login-password --region $REGION
    $loginPwd | docker login --username AWS --password-stdin "$ECR_REGISTRY"
    if ($LASTEXITCODE -ne 0) { throw "ECR login failed" }

    Log-Info "Building image from: $dockerContext"
    docker build -t "${ECR_REPO}:${IMAGE_TAG}" $dockerContext
    if ($LASTEXITCODE -ne 0) { throw "docker build failed" }

    docker tag "${ECR_REPO}:${IMAGE_TAG}" $IMAGE_URI
    docker push $IMAGE_URI
    if ($LASTEXITCODE -ne 0) { throw "docker push failed" }

    Log-Ok "Image pushed: $IMAGE_URI"
}

# --- STEP 3: SECRETS MANAGER -------------------------------------------------
Log-Step "Step 3/6 - Secrets Manager"

if ($SkipSecrets) {
    Log-Skip "SkipSecrets flag set - skipping secret creation"
} else {
    function Upsert-Secret {
        param([string]$Name, [string]$Description, [string]$Placeholder)
        $existing = Test-AWS @("secretsmanager", "describe-secret", "--secret-id", $Name,
            "--region", $REGION, "--output", "text")
        if ($existing) {
            Log-Skip "Secret '$Name' already exists"
        } else {
            Invoke-AWS @("secretsmanager", "create-secret",
                "--name", $Name,
                "--description", $Description,
                "--secret-string", $Placeholder,
                "--region", $REGION,
                "--output", "text") | Out-Null
            Log-Ok "Created secret: $Name"
            Write-Host "    *** ACTION REQUIRED: Update '$Name' with the real value ***" -ForegroundColor Red
        }
    }

    Upsert-Secret `
        -Name "$SECRET_PREFIX/lms-api-key" `
        -Description "Content Ingestion API key for X-API-Key header auth (staging)" `
        -Placeholder "REPLACE_WITH_REAL_API_KEY"

    Upsert-Secret `
        -Name "$SECRET_PREFIX/mongodb-uri" `
        -Description "MongoDB Atlas URI for Content Ingestion service (staging)" `
        -Placeholder "REPLACE_WITH_REAL_MONGODB_URI"

    Upsert-Secret `
        -Name "$SECRET_PREFIX/s3-cdn-base-url" `
        -Description "CDN base URL for course assets (staging)" `
        -Placeholder "REPLACE_WITH_REAL_CDN_URL"
}

# Fetch secret ARNs (needed for task definition)
# Use direct AWS CLI calls (not Test-AWS which returns bool) to get the ARN strings
$prev = $ErrorActionPreference; $ErrorActionPreference = 'SilentlyContinue'
$SECRET_API_KEY_ARN = (& aws secretsmanager describe-secret --secret-id "$SECRET_PREFIX/lms-api-key" --region $REGION --query "ARN" --output text 2>$null)
$SECRET_MONGO_ARN   = (& aws secretsmanager describe-secret --secret-id "$SECRET_PREFIX/mongodb-uri"  --region $REGION --query "ARN" --output text 2>$null)
$SECRET_CDN_ARN     = (& aws secretsmanager describe-secret --secret-id "$SECRET_PREFIX/s3-cdn-base-url" --region $REGION --query "ARN" --output text 2>$null)
$ErrorActionPreference = $prev

if (-not $DryRun) {
    if (-not $SECRET_API_KEY_ARN -or -not $SECRET_MONGO_ARN -or -not $SECRET_CDN_ARN) {
        throw "One or more secret ARNs could not be resolved. Ensure secrets exist in Secrets Manager before deploying."
    }
}

if (-not $DryRun) {
    Log-Info "Secret ARNs resolved:"
    Log-Info "  LMS_API_KEY     : $SECRET_API_KEY_ARN"
    Log-Info "  MONGODB_URI     : $SECRET_MONGO_ARN"
    Log-Info "  S3_CDN_BASE_URL : $SECRET_CDN_ARN"
}

 # Reuse the existing shared staging task role (nextgen-lms-ecs-task-role)
# No new IAM role is created; the old one is already attached to the staging cluster.
$RESOLVED_TASK_ROLE_ARN = $TASK_ROLE_ARN
Log-Skip "Using existing shared staging task role: $TASK_ROLE_ARN (no new role created)"

# --- STEP 4: CLOUDWATCH LOG GROUP --------------------------------------------
Log-Step "Step 4/6 - CloudWatch Log Group"

$prev = $ErrorActionPreference; $ErrorActionPreference = 'SilentlyContinue'
$existingLG = (& aws logs describe-log-groups `
    --log-group-name-prefix $LOG_GROUP --region $REGION `
    --query "logGroups[?logGroupName=='$LOG_GROUP'].logGroupName" --output text 2>$null)
$ErrorActionPreference = $prev
if ($existingLG -eq $LOG_GROUP) {
    Log-Skip "Log group '$LOG_GROUP' already exists"
} else {
    Invoke-AWS @("logs", "create-log-group",
        "--log-group-name", $LOG_GROUP,
        "--region", $REGION) | Out-Null
    Invoke-AWS @("logs", "put-retention-policy",
        "--log-group-name", $LOG_GROUP,
        "--retention-in-days", "30",
        "--region", $REGION) | Out-Null
    Log-Ok "Created log group: $LOG_GROUP with 30-day retention"
}

# --- STEP 5: SECURITY GROUP --------------------------------------------------
Log-Step "Step 5/6 - Security Group"

$prev = $ErrorActionPreference; $ErrorActionPreference = 'SilentlyContinue'
$existingSG = (& aws ec2 describe-security-groups `
    --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC_ID" `
    --region $REGION --query "SecurityGroups[0].GroupId" --output text 2>$null)
$ErrorActionPreference = $prev
if ($existingSG -and $existingSG -ne "None") {
    Log-Skip "Security group '$SG_NAME' already exists: $existingSG"
    $SG_ID = $existingSG
} else {
    $sgJson = Invoke-AWSCapture @("ec2", "create-security-group",
        "--group-name", $SG_NAME,
        "--description", "Content Ingestion staging service port 5009 from VPC only",
        "--vpc-id", $VPC_ID,
        "--region", $REGION,
        "--output", "json")

    if (-not $DryRun) {
        $SG_ID = ($sgJson | ConvertFrom-Json).GroupId
    } else {
        $SG_ID = "sg-DRYRUN"
    }

    # Allow port 5009 from within the VPC only (172.31.0.0/16)
    Invoke-AWS @("ec2", "authorize-security-group-ingress",
        "--group-id", $SG_ID,
        "--protocol", "tcp",
        "--port", "5009",
        "--cidr", "172.31.0.0/16",
        "--region", $REGION) | Out-Null

    Invoke-AWS @("ec2", "create-tags",
        "--resources", $SG_ID,
        "--tags", "Key=Name,Value=$SG_NAME", "Key=Environment,Value=staging",
        "--region", $REGION) | Out-Null

    Log-Ok "Created security group: $SG_ID (port 5009 open to VPC CIDR 10.0.0.0/16)"
}

# --- STEP 6: ECS TASK DEFINITION AND SERVICE ---------------------------------
Log-Step "Step 6/6 - ECS Task Definition and Service"

$resolvedTaskRoleArn = if ($DryRun) { "arn:aws:iam::${ACCOUNT_ID}:role/nextgen-lms-ecs-task-role" } else { $RESOLVED_TASK_ROLE_ARN }
$resolvedApiKeyArn   = if ($DryRun) { "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:PLACEHOLDER" } else { $SECRET_API_KEY_ARN }
$resolvedMongoArn    = if ($DryRun) { "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:PLACEHOLDER" } else { $SECRET_MONGO_ARN }
$resolvedCdnArn      = if ($DryRun) { "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:PLACEHOLDER" } else { $SECRET_CDN_ARN }

$taskDefJson = @"
{
  "family": "$TASK_DEF_FAMILY",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "$EXEC_ROLE_ARN",
  "taskRoleArn": "$resolvedTaskRoleArn",
  "containerDefinitions": [
    {
      "name": "content-ingestion",
      "image": "$IMAGE_URI",
      "essential": true,
      "portMappings": [
        { "containerPort": 5009, "protocol": "tcp" }
      ],
      "environment": [
        { "name": "AWS_REGION",       "value": "$REGION" },
        { "name": "PORT",             "value": "5009" },
        { "name": "LOG_FORMAT",       "value": "json" },
        { "name": "LOG_LEVEL",        "value": "INFO" },
        { "name": "MAX_UPLOAD_MB",    "value": "500" },
        { "name": "S3_ASSETS_BUCKET", "value": "$S3_BUCKET" },
        { "name": "DISABLE_AUTH",     "value": "false" }
      ],
      "secrets": [
        { "name": "LMS_API_KEY",     "valueFrom": "$resolvedApiKeyArn" },
        { "name": "MONGODB_URI",     "valueFrom": "$resolvedMongoArn" },
        { "name": "S3_CDN_BASE_URL", "valueFrom": "$resolvedCdnArn" }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group":         "$LOG_GROUP",
          "awslogs-region":        "$REGION",
          "awslogs-stream-prefix": "ecs",
          "awslogs-create-group":  "true"
        }
      },
      "healthCheck": {
        "command":     ["CMD-SHELL", "curl -f http://localhost:5009/api/v1/health || exit 1"],
        "interval":    30,
        "timeout":     5,
        "retries":     3,
        "startPeriod": 60
      }
    }
  ]
}
"@

$taskDefFile = [System.IO.Path]::GetTempFileName() + ".json"
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($taskDefFile, $taskDefJson, $utf8NoBom)

$tdResult = Invoke-AWSCapture @("ecs", "register-task-definition",
    "--cli-input-json", "file://$taskDefFile",
    "--region", $REGION,
    "--output", "json")

Remove-Item $taskDefFile -Force

if (-not $DryRun) {
    $TD_ARN = ($tdResult | ConvertFrom-Json).taskDefinition.taskDefinitionArn
    Log-Ok "Registered task definition: $TD_ARN"
} else {
    $TD_ARN = "arn:aws:ecs:${REGION}:${ACCOUNT_ID}:task-definition/${TASK_DEF_FAMILY}:1"
    Log-Ok "[DRY-RUN] Would register task definition: $TD_ARN"
}

# Create or update ECS Service
$prev = $ErrorActionPreference; $ErrorActionPreference = 'SilentlyContinue'
$existingServiceStatus = (& aws ecs describe-services `
    --cluster $ECS_CLUSTER --services $SERVICE_NAME --region $REGION `
    --query "services[0].status" --output text 2>$null)
$ErrorActionPreference = $prev

if ($existingServiceStatus -eq "ACTIVE") {
    Log-Skip "ECS service '$SERVICE_NAME' already exists - updating task definition"
    Invoke-AWS @("ecs", "update-service",
        "--cluster", $ECS_CLUSTER,
        "--service", $SERVICE_NAME,
        "--task-definition", $TD_ARN,
        "--region", $REGION,
        "--output", "text") | Out-Null
    Log-Ok "Service updated with new task definition"
} else {
    $netConfig = "awsvpcConfiguration={subnets=[$SUBNET_1,$SUBNET_2],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"
    Invoke-AWS @("ecs", "create-service",
        "--cluster", $ECS_CLUSTER,
        "--service-name", $SERVICE_NAME,
        "--task-definition", $TD_ARN,
        "--desired-count", "1",
        "--launch-type", "FARGATE",
        "--network-configuration", $netConfig,
        "--region", $REGION,
        "--output", "text") | Out-Null
    Log-Ok "Created ECS service: $SERVICE_NAME"
}

# --- SUMMARY -----------------------------------------------------------------
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Content Ingestion Service - Staging Deployment Complete" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Environment : Staging"
Write-Host " Cluster    : $ECS_CLUSTER"
Write-Host " Service    : $SERVICE_NAME"
Write-Host " Image      : $IMAGE_URI"
Write-Host " Subnets    : $SUBNET_1, $SUBNET_2 (private)"
Write-Host " Sec Group  : $SG_ID (port 5009, VPC-only)"
Write-Host " Logs       : $LOG_GROUP"
Write-Host ""
Write-Host " Internal URL (from other ECS services in same VPC):" -ForegroundColor Yellow
Write-Host "   http://<task-private-ip>:5009/api/v1/health" -ForegroundColor Yellow
Write-Host ""
if (-not $SkipSecrets) {
    Write-Host " *** IMPORTANT: Update these secrets with real values ***" -ForegroundColor Red
    Write-Host "   $SECRET_PREFIX/lms-api-key" -ForegroundColor Red
    Write-Host "   $SECRET_PREFIX/mongodb-uri" -ForegroundColor Red
    Write-Host "   $SECRET_PREFIX/s3-cdn-base-url" -ForegroundColor Red
    Write-Host ""
    Write-Host " Update a secret with:" -ForegroundColor Gray
    Write-Host "   aws secretsmanager put-secret-value --secret-id '$SECRET_PREFIX/lms-api-key' --secret-string 'YOUR_KEY' --region $REGION" -ForegroundColor Gray
}
Write-Host ""
Write-Host " Check service status:" -ForegroundColor Gray
Write-Host "   aws ecs describe-services --cluster $ECS_CLUSTER --services $SERVICE_NAME --region $REGION" -ForegroundColor Gray
Write-Host ""
Write-Host " Start (scale to 1 task):" -ForegroundColor Gray
Write-Host "   .\start-service.ps1" -ForegroundColor Gray
Write-Host " Stop (scale to 0):" -ForegroundColor Gray
Write-Host "   .\stop-service.ps1" -ForegroundColor Gray
Write-Host ""
