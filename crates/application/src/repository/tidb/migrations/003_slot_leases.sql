-- 003 (TiDB): dispatchers, slot lease ownership and expiry, fencing,
-- idempotency expiry (PLT-4631). Mirrors
-- ../../sqlite/migrations/003_slot_leases.sql. Expand only: a new table,
-- nullable columns or columns with a constant default (instant in TiDB),
-- and indexes (backfilled online by TiDB's add-index job).

CREATE TABLE IF NOT EXISTS dispatchers (
    id               VARCHAR(64)     NOT NULL,
    instance         VARCHAR(256)    NOT NULL,
    hostname         VARCHAR(256)    NOT NULL,
    pid              BIGINT UNSIGNED NOT NULL,
    started_at       VARCHAR(40)     NOT NULL,
    lease_expires_at VARCHAR(40)     NOT NULL,
    stopped_at       VARCHAR(40)     NULL,
    reclaimed_at     VARCHAR(40)     NULL,
    body             LONGTEXT        NOT NULL,
    PRIMARY KEY (id) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Columns first, then their indexes: TiDB v8.5 cannot index a column added
-- in the same ALTER TABLE (ERROR 1072).
ALTER TABLE invocations
    ADD COLUMN IF NOT EXISTS owner_id VARCHAR(64) NULL;

ALTER TABLE environments
    ADD COLUMN IF NOT EXISTS owner_id VARCHAR(64) NULL,
    ADD COLUMN IF NOT EXISTS fenced TINYINT NOT NULL DEFAULT 0;

ALTER TABLE leases
    ADD COLUMN IF NOT EXISTS owner_id VARCHAR(64) NULL,
    ADD COLUMN IF NOT EXISTS expires_at VARCHAR(40) NULL,
    ADD COLUMN IF NOT EXISTS reclaimed TINYINT NOT NULL DEFAULT 0;

ALTER TABLE idempotency
    ADD COLUMN IF NOT EXISTS expires_at VARCHAR(40) NULL;

ALTER TABLE invocations
    ADD INDEX IF NOT EXISTS invocations_owner_terminal (owner_id, terminal);

ALTER TABLE environments
    ADD INDEX IF NOT EXISTS environments_owner_terminal (owner_id, terminal),
    ADD INDEX IF NOT EXISTS environments_fenced_terminal (fenced, terminal);

ALTER TABLE leases
    ADD INDEX IF NOT EXISTS leases_released_expires (released, expires_at),
    ADD INDEX IF NOT EXISTS leases_owner_released (owner_id, released),
    ADD INDEX IF NOT EXISTS leases_environment_released (environment_id, released);

ALTER TABLE idempotency
    ADD INDEX IF NOT EXISTS idempotency_expires (expires_at);
