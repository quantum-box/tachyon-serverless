-- 006 (TiDB): asynchronous invocation inputs and the transactional outbox
-- (PLT-4639). Mirrors ../../sqlite/migrations/006_invoke_async_outbox.sql.
-- Expand only. The TiDB adapter does not implement the outbox repository
-- yet; the schema and its claim query are verified (docs/db-index-review.md).

CREATE TABLE IF NOT EXISTS invocation_inputs (
    invocation_id VARCHAR(64)     NOT NULL,
    tenant_id     VARCHAR(64)     NOT NULL,
    storage       VARCHAR(16)     NOT NULL,
    size_bytes    BIGINT UNSIGNED NOT NULL,
    digest        VARCHAR(80)     NOT NULL,
    inline_body   LONGBLOB        NULL,
    object_id     VARCHAR(64)     NULL,
    region        VARCHAR(32)     NULL,
    created_at    VARCHAR(40)     NOT NULL,
    PRIMARY KEY (invocation_id) NONCLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;

CREATE TABLE IF NOT EXISTS outbox (
    event_id         VARCHAR(64)     NOT NULL,
    tenant_id        VARCHAR(64)     NOT NULL,
    topic            VARCHAR(64)     NOT NULL,
    payload          LONGTEXT        NOT NULL,
    created_at       VARCHAR(40)     NOT NULL,
    sent             TINYINT         NOT NULL,
    publish_attempts BIGINT UNSIGNED NOT NULL,
    next_attempt_at  VARCHAR(40)     NOT NULL,
    claimed_by       VARCHAR(64)     NULL,
    claim_expires_at VARCHAR(40)     NULL,
    last_error       VARCHAR(512)    NULL,
    sent_at          VARCHAR(40)     NULL,
    queue_sequence   BIGINT UNSIGNED NULL,
    PRIMARY KEY (event_id) NONCLUSTERED,
    KEY outbox_sent_next (sent, next_attempt_at, created_at),
    KEY outbox_sent_created (sent, created_at)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;
