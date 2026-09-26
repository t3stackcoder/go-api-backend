-- 0001_init: framework tables (spec 6.2). All tables carry the mediator_ prefix.

CREATE TABLE mediator_outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      UUID        NOT NULL UNIQUE,
    topic         TEXT        NOT NULL,
    stream_key    TEXT        NOT NULL,
    seq           BIGINT      NOT NULL,
    partition     SMALLINT    NOT NULL,
    event_type    TEXT        NOT NULL,
    schema_ver    INT         NOT NULL DEFAULT 1,
    payload       JSONB       NOT NULL,
    headers       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX mediator_outbox_unpublished_idx
    ON mediator_outbox (topic, partition, id) WHERE published_at IS NULL;
CREATE UNIQUE INDEX mediator_outbox_key_seq_idx
    ON mediator_outbox (topic, stream_key, seq);
CREATE INDEX mediator_outbox_published_idx
    ON mediator_outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE mediator_stream_seq (
    topic       TEXT   NOT NULL,
    stream_key  TEXT   NOT NULL,
    next_seq    BIGINT NOT NULL,
    PRIMARY KEY (topic, stream_key)
);

CREATE TABLE mediator_inbox (
    consumer_group TEXT        NOT NULL,
    event_id       UUID        NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
CREATE INDEX mediator_inbox_processed_idx ON mediator_inbox (processed_at);

CREATE TABLE mediator_idempotency (
    scope         TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    request_hash  BYTEA       NOT NULL,
    response      JSONB,                       -- NULL until the handler succeeds
    hits          INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, key)
);
CREATE INDEX mediator_idempotency_expiry_idx ON mediator_idempotency (expires_at);

CREATE TABLE mediator_relay_cursor (
    topic           TEXT        NOT NULL,
    partition       SMALLINT    NOT NULL,
    last_outbox_id  BIGINT      NOT NULL,
    last_stream_id  TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

CREATE TABLE mediator_schema_version (
    version    INT         PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fencing tokens for partition leases (7.2). A Postgres sequence rather than a Redis
-- counter so that tokens stay monotonic across Redis data loss (7.7, G14).
CREATE SEQUENCE mediator_fencing_seq AS BIGINT;

-- down
DROP SEQUENCE IF EXISTS mediator_fencing_seq;
DROP TABLE IF EXISTS mediator_schema_version;
DROP TABLE IF EXISTS mediator_relay_cursor;
DROP TABLE IF EXISTS mediator_idempotency;
DROP TABLE IF EXISTS mediator_inbox;
DROP TABLE IF EXISTS mediator_stream_seq;
DROP TABLE IF EXISTS mediator_outbox;
