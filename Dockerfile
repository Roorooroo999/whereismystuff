# Where's My Stuff - Cloud Run Deployment
# Build: docker build -t wheres-my-stuff-api .
# Run:   docker run -p 8080:8080 wheres-my-stuff-api

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy API application
COPY api/server.py .

# Copy dashboard
COPY dashboard/ ./dashboard/

# Cloud Run uses PORT environment variable
ENV PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:${PORT}/api/health || exit 1

# Run the application
CMD exec uvicorn server:app --host 0.0.0.0 --port ${PORT}
