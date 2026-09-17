-- 008 (TiDB): asynchronous dispatch, retries, dead letters and redrives
-- (PLT-4640). Mirrors ../../sqlite/migrations/008_async_dispatch.sql. Expand
-- only: three new tables and one column with a constant default (instant in
-- TiDB). The TiDB adapter does not implement the dispatch repository; the
-- schema is verified and the dead-letter clause of the object collection
-- claim reads dead_letters.

CREATE TABLE IF NOT EXISTS async_dispatch (
    invocation_id    VARCHAR(64)     NOT NULL,
    tenant_id        VARCHAR(64)     NOT NULL,
    function_id      VARCHAR(64)     NOT NULL,
    state            VARCHAR(16)     NOT NULL,
    generation       BIGINT UNSIGNED NOT NULL,
    attempts         BIGINT UNSIGNED NOT NULL,
    deferrals        BIGINT UNSIGNED NOT NULL,
    claimed_by       VARCHAR(64)     NULL,
    claim_expires_at VARCHAR(40)     NULL,
    next_attempt_at  VARCHAR(40)     NULL,
    first_attempt_at VARCHAR(40)     NULL,
    last_attempt_at  VARCHAR(40)     NULL,
    updated_at       VARCHAR(40)     NOT NULL,
    last_error       LONGTEXT        NULL,
    PRIMARY KEY (invocation_id) NONCLUSTERED,
    KEY async_dispatch_state_claim (state, claim_expires_at)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;

CREATE TABLE IF NOT EXISTS dead_letters (
    id            VARCHAR(64)  NOT NULL,
    tenant_id     VARCHAR(64)  NOT NULL,
    function_id   VARCHAR(64)  NULL,
    invocation_id VARCHAR(64)  NULL,
    reason        VARCHAR(32)  NOT NULL,
    status        VARCHAR(16)  NOT NULL,
    origin_key    VARCHAR(192) NOT NULL,
    created_at    VARCHAR(40)  NOT NULL,
    body          LONGTEXT     NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY dead_letters_function_created (function_id, created_at, id),
    KEY dead_letters_invocation (invocation_id),
    UNIQUE KEY dead_letters_origin (origin_key),
    KEY dead_letters_tenant_status (tenant_id, status)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS redrives (
    id                   VARCHAR(64) NOT NULL,
    dead_letter_id       VARCHAR(64) NOT NULL,
    tenant_id            VARCHAR(64) NOT NULL,
    source_invocation_id VARCHAR(64) NOT NULL,
    invocation_id        VARCHAR(64) NOT NULL,
    created_at           VARCHAR(40) NOT NULL,
    body                 LONGTEXT    NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY redrives_dead_letter (dead_letter_id, created_at),
    KEY redrives_invocation (invocation_id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS generation BIGINT UNSIGNED NOT NULL DEFAULT 0;
