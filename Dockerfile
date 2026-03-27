FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg binary
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY src/ src/
COPY migrations/ migrations/
COPY tests/ tests/

CMD ["python", "-m", "src.main"]
