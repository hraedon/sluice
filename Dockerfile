# syntax=docker/dockerfile:1.7
# Multi-stage build matching the cert-watch family pattern.
# Builder installs into a venv, runtime copies only the venv (no build tools).

FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install .

# --- runtime ---------------------------------------------------------------

FROM python:3.13-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Debian security patches newer than the base digest.
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r sluice && useradd -r -g sluice -d /tmp sluice

COPY --from=builder /opt/venv /opt/venv

# Short git sha of the commit this image was built from (surfaced in
# /status.json and the dashboard header). Empty for local builds.
ARG GIT_SHA=""
ENV SLUICE_BUILD_SHA=$GIT_SHA

USER sluice
EXPOSE 8800

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8800/healthz').status==200 else 1)"

ENTRYPOINT ["sluice"]
