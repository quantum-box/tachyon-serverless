-- 005 (TiDB): object references and collection tombstones (PLT-4638).
-- Mirrors ../../sqlite/migrations/005_object_refs.sql. Expand only.

CREATE TABLE IF NOT EXISTS object_refs (
    object_id     VARCHAR(64) NOT NULL,
    tenant_id     VARCHAR(64) NOT NULL,
    region        VARCHAR(32) NOT NULL,
    invocation_id VARCHAR(64) NOT NULL,
    attached_at   VARCHAR(40) NOT NULL,
    PRIMARY KEY (object_id, invocation_id) NONCLUSTERED,
    KEY object_refs_invocation (invocation_id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS object_tombstones (
    object_id     VARCHAR(64) NOT NULL,
    tenant_id     VARCHAR(64) NOT NULL,
    region        VARCHAR(32) NOT NULL,
    reason        VARCHAR(16) NOT NULL,
    tombstoned_at VARCHAR(40) NOT NULL,
    PRIMARY KEY (object_id) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;
