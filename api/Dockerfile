# Where's My Stuff API - Cloud Run Deployment
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py .

# Copy dashboard (from parent directory context)
# Note: Build from project root with: docker build -f api/Dockerfile .
COPY dashboard/ ./dashboard/

# Cloud Run uses PORT environment variable
ENV PORT=8080

# Run the application
CMD exec uvicorn server:app --host 0.0.0.0 --port ${PORT}
