FROM docker.ci.artifacts.walmart.com/hub-docker-release-remote/library/python:3.12-slim

# Replace default Debian apt sources with Walmart internal ARK mirror
# (deb.debian.org is blocked from WCNP clusters)
RUN printf 'deb [trusted=yes] http://ark-repos.wal-mart.com/ark/apt/published/debian/12.0/direct/soe/noenv/os/ bookworm main\n\
deb [trusted=yes] http://ark-repos.wal-mart.com/ark/apt/published/debian/12.0/direct/soe/noenv/updates/ bookworm-updates main\n\
deb [trusted=yes] http://ark-repos.wal-mart.com/ark/apt/published/debian/12.0/direct/soe/noenv/security/ bookworm-security main\n' \
  > /etc/apt/sources.list

# System deps: gcc for cryptography/pandas native extensions + ca-certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cache friendly)
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy FastAPI server and its package init
COPY api/server.py .
COPY api/__init__.py .

# Copy dashboard HTML files served by FastAPI's FileResponse
# server.py checks /app/dashboard/index.html (cloud path) automatically
COPY dashboard/ ./dashboard/

# Non-root user (UID 10000, GID 10001) — WCNP PSP requirement
RUN groupadd -g 10001 appGrp && \
    useradd -u 10000 -g appGrp -s /sbin/nologin -d /tmp app && \
    chown -R 10000:10001 /app

USER 10000

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    WMS_CACHE_DIR=/tmp/wms_cache \
    GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp-sa-key.json

EXPOSE 8080

# Wait loop for Akeyless init container to finish mounting /etc/secrets/gcp-sa-key.json.
# 'sleep 5' was unreliable — on slow volumes the file arrives after 5s and BQ auth fails
# permanently until the next hourly retry, causing 503s on all API endpoints.
# This loop polls every 2s for up to 60s (30 attempts), then starts gunicorn regardless.
#
# UvicornWorker: FastAPI is ASGI (not WSGI) — must use UvicornWorker, NOT gthread
# --workers 1: single worker keeps all background BQ-loading threads in one process
# --timeout 600: cold-start BQ fetch spans 5 tables and can take 5–10 min total
CMD ["sh", "-c", \
     "i=0; until [ -f /etc/secrets/gcp-sa-key.json ] || [ $i -ge 30 ]; do echo \"[STARTUP] Waiting for Akeyless secret... attempt $i/30\"; sleep 2; i=$((i+1)); done; \
      [ -f /etc/secrets/gcp-sa-key.json ] && echo '[STARTUP] Secret mounted OK' || echo '[STARTUP] WARNING: Secret not found after 60s, starting anyway'; \
      exec gunicorn server:app \
       -k uvicorn.workers.UvicornWorker \
       --bind 0.0.0.0:8080 \
       --workers 1 \
       --timeout 600 \
       --forwarded-allow-ips='*' \
       --log-level info \
       --access-logfile -"]
