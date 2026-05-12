#!/usr/bin/env powershell
# =============================================================================
# CourseOnboarding - AWS Deployment Script
# Deploys the standalone course-onboarding service into the existing
# EduvateHub AWS account WITHOUT touching any existing infrastructure.
#
# What this script creates (and ONLY this):
#   1. ECR repository          - lms/course-onboarding
#   2. Secrets Manager entries - LMS_API_KEY, MONGODB_URI, S3_CDN_BASE_URL
#   3. IAM Task Role           - lms-course-onboarding-task-role-prod
#   4. CloudWatch Log Group    - /ecs/course-onboarding
#   5. Security Group          - lms-course-onboarding-sg-prod (port 5009)
#   6. ECS Task Definition     - course-onboarding-task-prod
#   7. ECS Fargate Service     - course-onboarding-prod-service
#      (on existing cluster: nextgen-lms-cluster-prod)
#
# Existing resources reused (read-only, never modified):
#   - VPC:              vpc-04310cb5dc299f90e  (lms-vpc-prod)
#   - Private Subnets:  subnet-011c21570906288d1 / subnet-030cb92e3e9897e93
#   - ECS Cluster:      nextgen-lms-cluster-prod
#   - Execution Role:   arn:aws:iam::129617679313:role/lms-ecs-task-execution-role-prod
#   - S3 Bucket:        eduvatehub-courseshells-prod
#
# Usage:
#   .\aws_infra\deploy.ps1
#   .\aws_infra\deploy.ps1 -DryRun          # validate only, no AWS calls
#   .\aws_infra\deploy.ps1 -SkipBuild       # skip docker build+push
#   .\aws_infra\deploy.ps1 -SkipSecrets     # secrets already created
# =============================================================================

param(
    [switch]$DryRun,
    [switch]$SkipBuild,
    [switch]$SkipSecrets
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Safe wrapper for "check if exists" AWS calls that may return non-zero
function Test-AWS {
    param([string[]]$AwsArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    $result = & aws @AwsArgs 2>$null
    $ok = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prev
    if ($ok) { return $result } else { return $null }
}

# --- CONFIG ------------------------------------------------------------------
$REGION          = "us-east-2"
$ACCOUNT_ID      = "129617679313"
$ECR_REGISTRY    = "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
$ECR_REPO        = "lms/course-onboarding"
$IMAGE_TAG       = "latest"
$IMAGE_URI       = "$ECR_REGISTRY/${ECR_REPO}:$IMAGE_TAG"

# Existing resources - DO NOT CHANGE
$VPC_ID          = "vpc-04310cb5dc299f90e"
$SUBNET_1        = "subnet-011c21570906288d1"
$SUBNET_2        = "subnet-030cb92e3e9897e93"
$ECS_CLUSTER     = "nextgen-lms-cluster-prod"
$EXEC_ROLE_ARN   = "arn:aws:iam::${ACCOUNT_ID}:role/lms-ecs-task-execution-role-prod"
$S3_BUCKET       = "eduvatehub-courseshells-prod"

# New resources to create
$SG_NAME         = "lms-course-onboarding-sg-prod"
$TASK_ROLE_NAME  = "lms-course-onboarding-task-role-prod"
$TASK_DEF_FAMILY = "course-onboarding-task-prod"
$SERVICE_NAME    = "course-onboarding-prod-service"
$LOG_GROUP       = "/ecs/course-onboarding"
$SECRET_PREFIX   = "/lms/prod/course-onboarding"

# --- HELPERS -----------------------------------------------------------------
function Log-Step { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Log-Ok   { param([string]$msg) Write-Host "    OK: $msg" -ForegroundColor Green }
function Log-Skip { param([string]$msg) Write-Host "    SKIP: $msg" -ForegroundColor Yellow }
function Log-Info { param([string]$msg) Write-Host "    $msg" -ForegroundColor Gray }

function Invoke-AWS {
    param([string[]]$AwsArgs)
    if ($DryRun) {
        Write-Host "    [DRY-RUN] aws $($AwsArgs -join ' ')" -ForegroundColor DarkGray
        return $null
    }
    $result = & aws @AwsArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    ERROR: $result" -ForegroundColor Red
        throw "AWS CLI command failed: aws $($AwsArgs -join ' ')"
    }
    return $result
}

# --- PRE-FLIGHT --------------------------------------------------------------
Log-Step "Pre-flight checks"

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
Log-Step "Step 1/7 - ECR Repository"

$existingRepo = Test-AWS @("ecr", "describe-repositories", "--repository-names", $ECR_REPO,
    "--region", $REGION, "--query", "repositories[0].repositoryUri", "--output", "text")

if ($existingRepo) {
    Log-Skip "ECR repo '$ECR_REPO' already exists: $existingRepo"
} else {
    Invoke-AWS @("ecr", "create-repository",
        "--repository-name", $ECR_REPO,
        "--region", $REGION,
        "--image-scanning-configuration", "scanOnPush=true",
        "--tags", "Key=Environment,Value=prod", "Key=Service,Value=course-onboarding",
        "--output", "text") | Out-Null
    Log-Ok "Created ECR repo: $ECR_REGISTRY/$ECR_REPO"
}

# --- STEP 2: DOCKER BUILD AND PUSH -------------------------------------------
Log-Step "Step 2/7 - Docker Build and Push"

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
Log-Step "Step 3/7 - Secrets Manager"

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
        -Description "CourseOnboarding API key for X-API-Key header auth" `
        -Placeholder "REPLACE_WITH_REAL_API_KEY"

    Upsert-Secret `
        -Name "$SECRET_PREFIX/mongodb-uri" `
        -Description "MongoDB Atlas URI for CourseOnboarding service" `
        -Placeholder "REPLACE_WITH_REAL_MONGODB_URI"

    Upsert-Secret `
        -Name "$SECRET_PREFIX/s3-cdn-base-url" `
        -Description "CDN base URL for course assets" `
        -Placeholder "REPLACE_WITH_REAL_CDN_URL"
}

# Fetch secret ARNs (needed for task definition)
$SECRET_API_KEY_ARN = Test-AWS @("secretsmanager", "describe-secret",
    "--secret-id", "$SECRET_PREFIX/lms-api-key",
    "--region", $REGION, "--query", "ARN", "--output", "text")
$SECRET_MONGO_ARN = Test-AWS @("secretsmanager", "describe-secret",
    "--secret-id", "$SECRET_PREFIX/mongodb-uri",
    "--region", $REGION, "--query", "ARN", "--output", "text")
$SECRET_CDN_ARN = Test-AWS @("secretsmanager", "describe-secret",
    "--secret-id", "$SECRET_PREFIX/s3-cdn-base-url",
    "--region", $REGION, "--query", "ARN", "--output", "text")

if (-not $DryRun) {
    Log-Info "Secret ARNs resolved:"
    Log-Info "  LMS_API_KEY     : $SECRET_API_KEY_ARN"
    Log-Info "  MONGODB_URI     : $SECRET_MONGO_ARN"
    Log-Info "  S3_CDN_BASE_URL : $SECRET_CDN_ARN"
}

# --- STEP 4: IAM TASK ROLE ---------------------------------------------------
Log-Step "Step 4/7 - IAM Task Role"

$existingRole = Test-AWS @("iam", "get-role", "--role-name", $TASK_ROLE_NAME,
    "--query", "Role.Arn", "--output", "text")
if ($existingRole) {
    Log-Skip "IAM role '$TASK_ROLE_NAME' already exists"
    $TASK_ROLE_ARN = $existingRole
} else {
    $trustPolicy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    $trustFile = [System.IO.Path]::GetTempFileName() + ".json"
    [System.IO.File]::WriteAllText($trustFile, $trustPolicy, [System.Text.Encoding]::UTF8)

    Invoke-AWS @("iam", "create-role",
        "--role-name", $TASK_ROLE_NAME,
        "--assume-role-policy-document", "file://$trustFile",
        "--description", "Task role for CourseOnboarding ECS service",
        "--output", "text") | Out-Null

    Remove-Item $trustFile -Force

    $inlinePolicy = "{`"Version`":`"2012-10-17`",`"Statement`":[{`"Sid`":`"S3Access`",`"Effect`":`"Allow`",`"Action`":[`"s3:GetObject`",`"s3:PutObject`",`"s3:DeleteObject`",`"s3:ListBucket`"],`"Resource`":[`"arn:aws:s3:::$S3_BUCKET`",`"arn:aws:s3:::$S3_BUCKET/*`"]},{`"Sid`":`"SecretsRead`",`"Effect`":`"Allow`",`"Action`":[`"secretsmanager:GetSecretValue`"],`"Resource`":`"arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:${SECRET_PREFIX}/*`"},{`"Sid`":`"CloudWatchLogs`",`"Effect`":`"Allow`",`"Action`":[`"logs:CreateLogStream`",`"logs:PutLogEvents`"],`"Resource`":`"arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP}:*`"}]}"

    $policyFile = [System.IO.Path]::GetTempFileName() + ".json"
    [System.IO.File]::WriteAllText($policyFile, $inlinePolicy, [System.Text.Encoding]::UTF8)

    Invoke-AWS @("iam", "put-role-policy",
        "--role-name", $TASK_ROLE_NAME,
        "--policy-name", "CourseOnboardingTaskPolicy",
        "--policy-document", "file://$policyFile") | Out-Null

    Remove-Item $policyFile -Force

    $TASK_ROLE_ARN = "arn:aws:iam::${ACCOUNT_ID}:role/$TASK_ROLE_NAME"
    Log-Ok "Created IAM role: $TASK_ROLE_ARN"
}

# --- STEP 5: CLOUDWATCH LOG GROUP --------------------------------------------
Log-Step "Step 5/7 - CloudWatch Log Group"

$existingLG = Test-AWS @("logs", "describe-log-groups",
    "--log-group-name-prefix", $LOG_GROUP,
    "--region", $REGION,
    "--query", "logGroups[?logGroupName=='$LOG_GROUP'].logGroupName",
    "--output", "text")
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

# --- STEP 6: SECURITY GROUP --------------------------------------------------
Log-Step "Step 6/7 - Security Group"

$existingSG = Test-AWS @("ec2", "describe-security-groups",
    "--filters", "Name=group-name,Values=$SG_NAME", "Name=vpc-id,Values=$VPC_ID",
    "--region", $REGION,
    "--query", "SecurityGroups[0].GroupId",
    "--output", "text")
if ($existingSG -and $existingSG -ne "None") {
    Log-Skip "Security group '$SG_NAME' already exists: $existingSG"
    $SG_ID = $existingSG
} else {
    $sgJson = Invoke-AWS @("ec2", "create-security-group",
        "--group-name", $SG_NAME,
        "--description", "CourseOnboarding service port 5009 from VPC only",
        "--vpc-id", $VPC_ID,
        "--region", $REGION,
        "--output", "json")

    if (-not $DryRun) {
        $SG_ID = ($sgJson | ConvertFrom-Json).GroupId
    } else {
        $SG_ID = "sg-DRYRUN"
    }

    # Allow port 5009 from within the VPC only (10.0.0.0/16)
    Invoke-AWS @("ec2", "authorize-security-group-ingress",
        "--group-id", $SG_ID,
        "--protocol", "tcp",
        "--port", "5009",
        "--cidr", "10.0.0.0/16",
        "--region", $REGION) | Out-Null

    Invoke-AWS @("ec2", "create-tags",
        "--resources", $SG_ID,
        "--tags", "Key=Name,Value=$SG_NAME", "Key=Environment,Value=prod",
        "--region", $REGION) | Out-Null

    Log-Ok "Created security group: $SG_ID (port 5009 open to VPC CIDR 10.0.0.0/16)"
}

# --- STEP 7: ECS TASK DEFINITION AND SERVICE ---------------------------------
Log-Step "Step 7/7 - ECS Task Definition and Service"

$resolvedTaskRoleArn = if ($DryRun) { "arn:aws:iam::${ACCOUNT_ID}:role/$TASK_ROLE_NAME" } else { $TASK_ROLE_ARN }
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
      "name": "course-onboarding",
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
        { "name": "S3_ASSETS_BUCKET", "value": "$S3_BUCKET" }
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
[System.IO.File]::WriteAllText($taskDefFile, $taskDefJson, [System.Text.Encoding]::UTF8)

$tdResult = Invoke-AWS @("ecs", "register-task-definition",
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
$existingServiceStatus = Test-AWS @("ecs", "describe-services",
    "--cluster", $ECS_CLUSTER, "--services", $SERVICE_NAME,
    "--region", $REGION, "--query", "services[0].status", "--output", "text")

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
    $netConfig = "awsvpcConfiguration={subnets=[$SUBNET_1,$SUBNET_2],securityGroups=[$SG_ID],assignPublicIp=DISABLED}"
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
Write-Host " CourseOnboarding Deployment Complete" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host " Cluster  : $ECS_CLUSTER"
Write-Host " Service  : $SERVICE_NAME"
Write-Host " Image    : $IMAGE_URI"
Write-Host " Subnets  : $SUBNET_1, $SUBNET_2 (private)"
Write-Host " Sec Group: $SG_ID (port 5009, VPC-only)"
Write-Host " Logs     : $LOG_GROUP"
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
