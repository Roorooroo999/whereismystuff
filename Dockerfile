# Where's My Stuff - WCNP Deployment
# Uses Walmart GBI base image (Alpine) — includes non-root user + internal CA certs
# WCNP requires non-root (UID 10000). Root containers are rejected by Pod Security Policy.
#
# Build:  podman build -t wheres-my-stuff .
# Verify: podman run wheres-my-stuff id   # must show uid=10000

# ── Stage 1: dependency builder ──────────────────────────────────────────────
FROM docker.ci.artifacts.walmart.com/wce-docker/alpine:3-main AS builder

LABEL maintainer="r0c0jug@walmart.com"

# Switch to root only to install OS packages
USER root

# Install Python 3 + pip + build tools
RUN apk add --no-cache python3 py3-pip python3-dev gcc musl-dev linux-headers

WORKDIR /app

# Install Python dependencies into /install so we can copy into final stage
COPY --chown=10000:10000 api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: minimal runtime image ───────────────────────────────────────────
FROM docker.ci.artifacts.walmart.com/wce-docker/alpine:3-main

USER root

# Python runtime only — no build tools, no pip
RUN apk add --no-cache python3

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder --chown=10000:10000 /install /usr/local

# Copy application code
COPY --chown=10000:10000 api/server.py ./server.py
COPY --chown=10000:10000 api/__init__.py ./__init__.py
COPY --chown=10000:10000 dashboard/ ./dashboard/

# WCNP: use non-root user (GBI provides uid 10000)
USER 10000

# Non-privileged port (ports < 1024 require root)
EXPOSE 8080

# Environment defaults (override via ConfigMap / Helm values in WCNP)
ENV PORT=8080 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Startup command
CMD ["python3", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
