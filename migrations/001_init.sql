-- 001_init.sql
-- Schema for the CodeSystem change poller

CREATE SCHEMA IF NOT EXISTS poller;

-- ──────────────────────────────────────────────
-- Last-known-good snapshot of each CodeSystem
-- ──────────────────────────────────────────────
CREATE TABLE poller.codesystem_sync_state (
    system_url      TEXT PRIMARY KEY,
    version         TEXT,
    resource_hash   TEXT NOT NULL,          -- SHA-256 of the raw (or canonical) response
    resource_json   JSONB NOT NULL,         -- full FHIR CodeSystem JSON for recovery
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ──────────────────────────────────────────────
-- Per-concept row-level state for diffing
-- ──────────────────────────────────────────────
CREATE TABLE poller.codesystem_concept_state (
    system_url      TEXT NOT NULL REFERENCES poller.codesystem_sync_state(system_url) ON DELETE CASCADE,
    code            TEXT NOT NULL,
    display         TEXT,
    definition      TEXT,
    concept_hash    TEXT NOT NULL,           -- SHA-256 of the canonical concept JSON
    properties      JSONB DEFAULT '{}',      -- property[] + designation[] merged
    parent_code     TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (system_url, code)
);

CREATE INDEX idx_concept_state_system ON poller.codesystem_concept_state(system_url);

-- ──────────────────────────────────────────────
-- Transactional outbox for change events
-- ──────────────────────────────────────────────
CREATE TABLE poller.change_outbox (
    id              BIGSERIAL PRIMARY KEY,
    system_url      TEXT NOT NULL,
    change_type     TEXT NOT NULL CHECK (change_type IN ('concept_added', 'concept_modified', 'concept_removed')),
    code            TEXT,
    old_value       JSONB,
    new_value       JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published       BOOLEAN NOT NULL DEFAULT false,
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON poller.change_outbox(published) WHERE published = false;
