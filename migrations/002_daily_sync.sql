-- 002_daily_sync.sql
-- CodeSystem metadata/lifecycle, concept soft delete, and per-run summaries.
--
-- Idempotent: the migrate service re-runs every file on each startup.

-- ──────────────────────────────────────────────
-- CodeSystem-level metadata and lifecycle
-- ──────────────────────────────────────────────
ALTER TABLE poller.codesystem_sync_state
    ADD COLUMN IF NOT EXISTS endpoint_url  TEXT,          -- URL actually polled
    ADD COLUMN IF NOT EXISTS name          TEXT,
    ADD COLUMN IF NOT EXISTS title         TEXT,
    ADD COLUMN IF NOT EXISTS content       TEXT,          -- CodeSystem.content (complete, fragment, ...)
    ADD COLUMN IF NOT EXISTS active        BOOLEAN NOT NULL DEFAULT true,   -- false = no longer configured
    ADD COLUMN IF NOT EXISTS last_seen_at  TIMESTAMPTZ,   -- last successful fetch, even if unchanged
    ADD COLUMN IF NOT EXISTS removed_at    TIMESTAMPTZ;

-- ──────────────────────────────────────────────
-- Concept soft delete: removed concepts are kept with active = false
-- ──────────────────────────────────────────────
ALTER TABLE poller.codesystem_concept_state
    ADD COLUMN IF NOT EXISTS active      BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS removed_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_concept_state_active
    ON poller.codesystem_concept_state(system_url) WHERE active;

-- ──────────────────────────────────────────────
-- One row per scheduled run, for monitoring
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS poller.sync_run (
    id           BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    discovered   INT,                                   -- number of CodeSystems in this run
    unchanged    INT NOT NULL DEFAULT 0,
    changed      INT NOT NULL DEFAULT 0,
    skipped      INT NOT NULL DEFAULT 0,
    failed       INT NOT NULL DEFAULT 0,
    details      JSONB NOT NULL DEFAULT '[]'            -- [{system_url, status, added, modified, removed, error}]
);
