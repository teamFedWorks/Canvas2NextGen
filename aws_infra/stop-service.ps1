#!/usr/bin/env powershell
# =============================================================================
# Content Ingestion Service - Stop (Scale to Zero)
#
# This is an ADHOC service. It should NOT run continuously in staging.
# Start it only when you need to ingest courses, then stop it when done.
#
# No resources are deleted - the service, task definition, secrets, IAM role,
# and ECR image all remain intact. Restart with start-service.ps1.
#
# Cost saving: ~$30/month saved per environment when stopped.
#
# Usage:
#   .\aws_infra\stop-service.ps1                    # stops staging (default)
#   .\aws_infra\stop-service.ps1 -Env prod          # stops prod
#   .\aws_infra\stop-service.ps1 -Env staging -Wait # stops and waits for 0 tasks
# =============================================================================

param(
    [ValidateSet("staging", "prod")]
    [string]$Env = "staging",
    [switch]$Wait
)

$ErrorActionPreference = "Stop"
$REGION      = "us-east-2"
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
Write-Host "============================================" -ForegroundColor Yellow
Write-Host " Stopping $DISPLAY_NAME [$Env]" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host " Cluster : $CLUSTER"
Write-Host " Service : $SERVICE"
Write-Host ""

# Check current state
$current = aws ecs describe-services `
    --cluster $CLUSTER `
    --services $SERVICE `
    --region $REGION `
    --query "services[0].{Status:status,Desired:desiredCount,Running:runningCount}" `
    --output json 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Service not found or AWS error." -ForegroundColor Red
    Write-Host $current
    exit 1
}

$svc = $current | ConvertFrom-Json

if ($svc.Status -ne "ACTIVE") {
    Write-Host "Service status is '$($svc.Status)' - nothing to stop." -ForegroundColor Yellow
    exit 0
}

if ($svc.Desired -eq 0) {
    Write-Host "Service is already stopped (desired=0, running=$($svc.Running))." -ForegroundColor Yellow
    exit 0
}

Write-Host "Current state: desired=$($svc.Desired), running=$($svc.Running)"
Write-Host "Scaling to 0..." -ForegroundColor Cyan

aws ecs update-service `
    --cluster $CLUSTER `
    --service $SERVICE `
    --desired-count 0 `
    --region $REGION `
    --output text | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to scale service." -ForegroundColor Red
    exit 1
}

Write-Host "Scale-to-zero command sent." -ForegroundColor Green

if ($Wait) {
    Write-Host "Waiting for all tasks to stop..."
    for ($i = 1; $i -le 20; $i++) {
        Start-Sleep -Seconds 10
        $running = aws ecs describe-services `
            --cluster $CLUSTER `
            --services $SERVICE `
            --region $REGION `
            --query "services[0].runningCount" `
            --output text 2>&1
        $ts = Get-Date -Format "HH:mm:ss"
        Write-Host "[$ts] Running tasks: $running"
        if ($running -eq "0") {
            Write-Host ""
            Write-Host "All tasks stopped." -ForegroundColor Green
            break
        }
    }
}

Write-Host ""
Write-Host " $DISPLAY_NAME stopped. No Fargate charges while at 0 tasks."
Write-Host " NOTE: This is an ADHOC service. Start it only when ingestion is needed."
Write-Host " To start: .\aws_infra\start-service.ps1 -Env $Env"
Write-Host ""
