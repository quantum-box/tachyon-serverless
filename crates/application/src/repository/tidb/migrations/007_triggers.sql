-- 007 (TiDB): cron and signed webhook triggers (PLT-4641). Mirrors
-- ../../sqlite/migrations/007_triggers.sql. Expand only. The TiDB adapter
-- does not implement the trigger repository yet; the schema and the fire
-- uniqueness are verified (docs/db-index-review.md).
--
-- SQLite's partial unique index trigger_fires_signature (WHERE
-- signature_digest IS NOT NULL) is a plain unique key here: MySQL and TiDB
-- unique keys already treat NULLs as distinct, which is the same guarantee.

CREATE TABLE IF NOT EXISTS triggers (
    id            VARCHAR(64)     NOT NULL,
    tenant_id     VARCHAR(64)     NOT NULL,
    function_id   VARCHAR(64)     NOT NULL,
    kind          VARCHAR(16)     NOT NULL,
    status        VARCHAR(16)     NOT NULL,
    generation    BIGINT UNSIGNED NOT NULL,
    next_fire_at  VARCHAR(40)     NULL,
    secret_sealed LONGBLOB        NULL,
    created_at    VARCHAR(40)     NOT NULL,
    updated_at    VARCHAR(40)     NOT NULL,
    body          LONGTEXT        NOT NULL,
    PRIMARY KEY (id) CLUSTERED,
    KEY triggers_function (function_id, status),
    KEY triggers_due (kind, status, next_fire_at)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

CREATE TABLE IF NOT EXISTS trigger_fires (
    trigger_id       VARCHAR(64)  NOT NULL,
    fire_key         VARCHAR(160) NOT NULL,
    tenant_id        VARCHAR(64)  NOT NULL,
    kind             VARCHAR(16)  NOT NULL,
    scheduled_at     VARCHAR(40)  NULL,
    event_id         VARCHAR(128) NULL,
    signature_digest VARCHAR(80)  NULL,
    invocation_id    VARCHAR(64)  NULL,
    outcome          VARCHAR(16)  NOT NULL,
    reason           VARCHAR(512) NULL,
    created_at       VARCHAR(40)  NOT NULL,
    PRIMARY KEY (trigger_id, fire_key) NONCLUSTERED,
    UNIQUE KEY trigger_fires_signature (trigger_id, signature_digest),
    KEY trigger_fires_created (kind, created_at)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS trigger_scheduler (
    name        VARCHAR(32) NOT NULL,
    owner_id    VARCHAR(64) NOT NULL,
    acquired_at VARCHAR(40) NOT NULL,
    expires_at  VARCHAR(40) NOT NULL,
    PRIMARY KEY (name) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;
