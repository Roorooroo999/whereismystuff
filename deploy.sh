#!/bin/bash
# Where's My Stuff - Cloud Run Deployment Script
# Run from the project root directory

set -e

PROJECT_ID="wmt-instockinventory-datamart"
SERVICE_NAME="wheres-my-stuff-api"
REGION="us-central1"

echo "=========================================="
echo "Where's My Stuff - Cloud Run Deployment"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Service: $SERVICE_NAME"
echo "Region:  $REGION"
echo ""

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --source . \
  --region $REGION \
  --allow-unauthenticated \
  --project $PROJECT_ID \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 10 \
  --timeout 300

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --project $PROJECT_ID --format 'value(status.url)')

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Dashboard:  ${SERVICE_URL}/dashboard"
echo "API Docs:   ${SERVICE_URL}/docs"
echo "Health:     ${SERVICE_URL}/api/health"
echo ""
