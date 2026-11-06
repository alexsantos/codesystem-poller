"""Migrations other than 001 must be safe to re-run (the migrate service re-applies all files)."""

import psycopg

from tests.conftest import MIGRATIONS, apply_migrations


def test_later_migrations_are_idempotent(pg_url):
    with psycopg.connect(pg_url) as conn:
        apply_migrations(conn, [f for f in MIGRATIONS if not f.name.startswith("001_")])
        apply_migrations(conn, [f for f in MIGRATIONS if not f.name.startswith("001_")])


def test_expected_columns_exist(pg):
    pg.execute(
        """
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = 'poller'
        """
    )
    cols = {(r["table_name"], r["column_name"]) for r in pg.fetchall()}
    for expected in [
        ("codesystem_sync_state", "last_seen_at"),
        ("codesystem_sync_state", "active"),
        ("codesystem_sync_state", "content"),
        ("codesystem_concept_state", "active"),
        ("codesystem_concept_state", "removed_at"),
        ("sync_run", "details"),
    ]:
        assert expected in cols
