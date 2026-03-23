# ============================================
# Stage 1: Builder — install dependencies with uv
# ============================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source and install project
COPY . .
RUN uv sync --frozen --no-dev

# ============================================
# Stage 2: Runtime
# ============================================
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy virtual environment and source from builder
COPY --from=builder /app /app

# Create non-root user
RUN groupadd --system app && useradd --system --gid app --create-home --home-dir /home/app app \
    && chown -R app:app /app

# Drop privileges
USER app

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Expose port
EXPOSE 8000

# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
