# ── Build stage ──────────────────────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# libpq5 is required by psycopg[binary]
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Non-root user for least-privilege execution
RUN useradd --no-create-home --system appuser

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src/ src/
COPY migrations/ migrations/

USER appuser

CMD ["python", "-m", "src.main"]