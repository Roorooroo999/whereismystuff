#!/bin/bash
# ============================================================
# Deploy Where's My Stuff API to Google Cloud Run
# ============================================================

# Configuration
PROJECT_ID="wmt-instockinventory-datamart"
REGION="us-central1"
SERVICE_NAME="wheres-my-stuff-api"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "🚀 Deploying Where's My Stuff API to Cloud Run..."
echo "   Project: ${PROJECT_ID}"
echo "   Region:  ${REGION}"
echo "   Service: ${SERVICE_NAME}"
echo ""

# Build the container image
echo "📦 Building container image..."
gcloud builds submit --tag ${IMAGE_NAME} .

# Deploy to Cloud Run
echo "☁️  Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 10 \
  --timeout 300 \
  --set-env-vars "PROJECT_ID=${PROJECT_ID}"

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format 'value(status.url)')

echo ""
echo "✅ Deployment complete!"
echo ""
echo "🌐 API URL: ${SERVICE_URL}"
echo "📖 API Docs: ${SERVICE_URL}/docs"
echo ""
echo "📋 Next steps:"
echo "   1. Update dashboard/preview_dashboard.html"
echo "   2. Change API_BASE_URL to: ${SERVICE_URL}"
echo ""
