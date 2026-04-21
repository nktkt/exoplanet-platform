# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# Stage 1: builder - compile dependencies into a wheelhouse.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for native wheels (psycopg2, scipy, etc. are wheels on slim but
# pin a safety net for source builds).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy minimal metadata first for a cacheable layer.
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip build \
 && pip wheel --wheel-dir /wheels .


# ---------------------------------------------------------------------------
# Stage 2: runtime - slim image with only what the app needs.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    EXOPLANET_ENVIRONMENT=production \
    EXOPLANET_API__HOST=0.0.0.0 \
    EXOPLANET_API__PORT=8000

# libpq5 needed by psycopg2-binary at runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq5 \
 && rm -rf /var/lib/apt/lists/*

# Non-root user.
RUN groupadd --system app && useradd --system --gid app --home /home/app --create-home app

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links=/wheels exoplanet-platform \
 && rm -rf /wheels

# Ensure the data dir exists and is writable by the non-root user.
RUN mkdir -p /app/data && chown -R app:app /app
USER app

EXPOSE 8000

# Default CMD runs the FastAPI server via the console script entry point.
CMD ["exoplanet-api"]
