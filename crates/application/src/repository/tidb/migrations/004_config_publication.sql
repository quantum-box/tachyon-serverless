-- 004 (TiDB): generation stamps of the published configuration (PLT-4636).
-- Mirrors ../../sqlite/migrations/004_config_publication.sql. Expand only.
-- entry_key is 512 characters, 2048 bytes in utf8mb4: under TiDB's 3072-byte
-- key limit.

CREATE TABLE IF NOT EXISTS config_publication (
    entry_key       VARCHAR(512)    NOT NULL,
    generation      BIGINT UNSIGNED NOT NULL,
    natural_version BIGINT UNSIGNED NOT NULL,
    digest          VARCHAR(80)     NOT NULL,
    body            LONGTEXT        NULL,
    PRIMARY KEY (entry_key) CLUSTERED,
    KEY config_publication_generation (generation)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;
