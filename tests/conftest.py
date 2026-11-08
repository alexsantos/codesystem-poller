"""
Shared fixtures.

`pg` gives a cursor on a real PostgreSQL with all migrations applied to a
fresh `poller` schema. It needs TEST_DATABASE_URL (a throwaway database —
the schema is dropped!) and is skipped otherwise, e.g.:

    docker compose run --rm -e TEST_DATABASE_URL=$DATABASE_URL poller python -m pytest tests/ -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

MIGRATIONS = sorted((Path(__file__).parent.parent / "migrations").glob("*.sql"))


def apply_migrations(conn, files=MIGRATIONS) -> None:
    for f in files:
        conn.execute(f.read_text())
    conn.commit()


@pytest.fixture
def pg_url(monkeypatch):
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set")

    import psycopg
    from psycopg.rows import dict_row

    from src.config import settings

    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DROP SCHEMA IF EXISTS poller CASCADE")
        conn.commit()
        apply_migrations(conn)

    monkeypatch.setattr(settings, "database_url", url)
    return url


@pytest.fixture
def pg(pg_url):
    """Autocommit cursor for arranging / asserting state in tests."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(pg_url, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            yield cur
