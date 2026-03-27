"""PostgreSQL connection pool and transaction helpers using psycopg3."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from src.config import settings

logger = logging.getLogger(__name__)

_pool: psycopg.Connection | None = None


def get_connection() -> psycopg.Connection:
    """Return a new connection (caller is responsible for closing)."""
    return psycopg.connect(settings.database_url, row_factory=dict_row)


@contextmanager
def transaction() -> Generator[psycopg.Cursor, None, None]:
    """
    Context manager that yields a cursor inside a transaction.
    Commits on clean exit, rolls back on exception.
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def check_health() -> bool:
    """Quick connectivity check."""
    try:
        conn = get_connection()
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception as exc:
        logger.error("Database health check failed: %s", exc)
        return False
