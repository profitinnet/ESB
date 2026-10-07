CREATE TABLE IF NOT EXISTS delivery_state (
    system TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (system, entity_id)
);
