#!/usr/bin/env powershell
# =============================================================================
# Content Ingestion Service - Start (Adhoc)
#
# This is an ADHOC service. Start it only when you need to ingest courses.
# Stop it when ingestion is complete using stop-service.ps1.
#
# Usage:
#   .\aws_infra\start-service.ps1                       # starts staging (default)
#   .\aws_infra\start-service.ps1 -Env prod             # starts prod
#   .\aws_infra\start-service.ps1 -Env staging -Count 2 # starts with 2 tasks
# =============================================================================

param(
    [ValidateSet("staging", "prod")]
    [string]$Env = "staging",
    [int]$Count = 1
)

$ErrorActionPreference = "Stop"
$REGION       = "us-east-2"
$DISPLAY_NAME = "Content Ingestion Service"

# Resolve cluster and service name based on environment
if ($Env -eq "staging") {
    $CLUSTER = "nextgen-lms-ecs-staging"
    $SERVICE = "course-onboarding-staging-service"
} else {
    $CLUSTER = "nextgen-lms-cluster-prod"
    $SERVICE = "course-onboarding-prod-service"
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Starting $DISPLAY_NAME [$Env]" -ForegroundColor Cyan
Write-Host " (ADHOC - remember to stop when done)" -ForegroundColor DarkCyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Cluster : $CLUSTER"
Write-Host " Service : $SERVICE"
Write-Host " Tasks   : $Count"
Write-Host ""

# Check current state
$current = aws ecs describe-services `
    --cluster $CLUSTER `
    --services $SERVICE `
    --region $REGION `
    --query "services[0].{Status:status,Desired:desiredCount,Running:runningCount,TaskDef:taskDefinition}" `
    --output json 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Service not found or AWS error." -ForegroundColor Red
    Write-Host $current
    exit 1
}

$svc = $current | ConvertFrom-Json

if ($svc.Status -ne "ACTIVE") {
    Write-Host "ERROR: Service status is '$($svc.Status)'. Cannot start." -ForegroundColor Red
    exit 1
}

if ($svc.Desired -ge $Count -and $svc.Running -ge $Count) {
    Write-Host "Service is already running (desired=$($svc.Desired), running=$($svc.Running))." -ForegroundColor Yellow
    exit 0
}

$shortTD = $svc.TaskDef -replace ".*task-definition/", ""
Write-Host "Current state: desired=$($svc.Desired), running=$($svc.Running)"
Write-Host "Task definition: $shortTD"
Write-Host "Scaling to $Count task(s)..." -ForegroundColor Cyan

aws ecs update-service `
    --cluster $CLUSTER `
    --service $SERVICE `
    --desired-count $Count `
    --region $REGION `
    --output text | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to scale service." -ForegroundColor Red
    exit 1
}

Write-Host "Scale-up command sent. Waiting for task to become HEALTHY..."
Write-Host "(Health check starts after 60s startup period)"
Write-Host ""

# Poll until HEALTHY or timeout
$healthy = $false
for ($i = 1; $i -le 24; $i++) {
    Start-Sleep -Seconds 15

    $taskArn = aws ecs list-tasks `
        --cluster $CLUSTER `
        --service-name $SERVICE `
        --region $REGION `
        --query "taskArns[0]" `
        --output text 2>&1

    if (-not $taskArn -or $taskArn -eq "None") {
        $ts = Get-Date -Format "HH:mm:ss"
        Write-Host "[$ts] Waiting for task to start..."
        continue
    }

    $task = aws ecs describe-tasks `
        --cluster $CLUSTER `
        --tasks $taskArn `
        --region $REGION `
        --query "tasks[0].{Status:lastStatus,Health:healthStatus}" `
        --output json 2>&1 | ConvertFrom-Json

    $ts = Get-Date -Format "HH:mm:ss"
    Write-Host "[$ts] Task=$($task.Status) | Health=$($task.Health)"

    if ($task.Health -eq "HEALTHY") {
        $healthy = $true
        break
    }

    if ($task.Status -eq "STOPPED") {
        Write-Host ""
        Write-Host "ERROR: Task stopped unexpectedly. Check logs:" -ForegroundColor Red
        Write-Host "  aws logs tail /ecs/course-onboarding-$Env --region $REGION --since 5m"
        exit 1
    }
}

Write-Host ""
if ($healthy) {
    $taskArn = aws ecs list-tasks `
        --cluster $CLUSTER `
        --service-name $SERVICE `
        --region $REGION `
        --query "taskArns[0]" `
        --output text

    $ip = aws ecs describe-tasks `
        --cluster $CLUSTER `
        --tasks $taskArn `
        --region $REGION `
        --query "tasks[0].attachments[0].details[?name=='privateIPv4Address'].value|[0]" `
        --output text

    Write-Host "============================================" -ForegroundColor Green
    Write-Host " $DISPLAY_NAME is HEALTHY" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host " Private IP  : $ip"
    Write-Host " Health URL  : http://${ip}:5009/api/v1/health"
    Write-Host " Logs        : aws logs tail /ecs/course-onboarding-$Env --region $REGION --follow"
    Write-Host ""
    Write-Host " REMINDER: Stop this service when ingestion is complete:" -ForegroundColor Yellow
    Write-Host "   .\aws_infra\stop-service.ps1 -Env $Env" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Host "WARNING: Timed out waiting for HEALTHY status." -ForegroundColor Yellow
    Write-Host "The task may still be starting. Check status with:"
    Write-Host "  aws ecs describe-services --cluster $CLUSTER --services $SERVICE --region $REGION"
}
