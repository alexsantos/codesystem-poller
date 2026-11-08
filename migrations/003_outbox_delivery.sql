-- 003_outbox_delivery.sql
-- Guaranteed (at-least-once) delivery of outbox rows.
--
-- The relay seals unpublished change_outbox rows into an outbox_batch whose
-- Bundle is frozen in `bundle`. Every delivery attempt sends exactly that
-- Bundle, whose id (= MessageHeader.id = outbox_batch.id) is the receiver's
-- deduplication key. Retry state lives here, so it survives restarts.
--
-- Idempotent: the migrate service re-runs every file on each startup.

CREATE TABLE IF NOT EXISTS poller.outbox_batch (
    id               UUID PRIMARY KEY,                  -- Bundle.id and MessageHeader.id
    seq              BIGSERIAL NOT NULL UNIQUE,         -- delivery order within a CodeSystem
    system_url       TEXT NOT NULL,
    bundle           JSONB NOT NULL,                    -- frozen at seal time, resent verbatim
    row_count        INT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivered', 'dead', 'discarded')),
    attempts         INT NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at  TIMESTAMPTZ,
    last_error       TEXT,
    delivered_to     TEXT,                              -- rabbitmq | fhir
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_batch_open
    ON poller.outbox_batch(system_url, seq) WHERE status IN ('pending', 'dead');

ALTER TABLE poller.change_outbox
    ADD COLUMN IF NOT EXISTS batch_id UUID REFERENCES poller.outbox_batch(id);

CREATE INDEX IF NOT EXISTS idx_outbox_unsealed
    ON poller.change_outbox(id) WHERE published = false AND batch_id IS NULL;
