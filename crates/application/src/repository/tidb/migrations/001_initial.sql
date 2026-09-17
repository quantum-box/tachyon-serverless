-- 001 (TiDB): the control-plane and cell-local tables. Mirrors
-- ../../sqlite/migrations/001_initial.sql column for column, with the same
-- primary keys, unique keys and indexes (docs/db-index-review.md).
--
-- Rules for this directory (docs/adr/0003 「TiDB 検証」):
--   * TiDB DDL is not transactional. Every statement is idempotent
--     (IF NOT EXISTS) and additive, so a migration that fails half way can be
--     re-run, and the previous binary keeps working on the partly expanded
--     schema. schema_version only advances once every statement succeeded.
--   * One DDL per statement, statements end with a semicolon at the end of a
--     line, and comments never contain one.
--   * Every text column that is compared is utf8mb4_bin (byte order and case
--     sensitivity like SQLite). Timestamps stay fixed-width RFC 3339 UTC
--     strings so both adapters share the encoding.
--   * u64 counters and reuse-key versions are BIGINT UNSIGNED.
--   * body columns are LONGTEXT (an inline output can exceed TEXT).
--   * Tables keyed by a time-ordered ULID that take one row per invocation
--     use a NONCLUSTERED primary key with SHARD_ROW_ID_BITS, so row data does
--     not pile up in the last region. Small control tables stay clustered.

CREATE TABLE IF NOT EXISTS store_meta (
    meta_key   VARCHAR(64) NOT NULL,
    meta_value LONGTEXT    NOT NULL,
    PRIMARY KEY (meta_key) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

CREATE TABLE IF NOT EXISTS functions (
    id         VARCHAR(64)  NOT NULL,
    tenant_id  VARCHAR(64)  NOT NULL,
    name       VARCHAR(128) NOT NULL,
    live_name  VARCHAR(128) NULL,
    created_at VARCHAR(40)  NOT NULL,
    body       LONGTEXT     NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    UNIQUE KEY functions_tenant_live_name (tenant_id, live_name),
    KEY functions_tenant_created (tenant_id, created_at, id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS revisions (
    id          VARCHAR(64)     NOT NULL,
    function_id VARCHAR(64)     NOT NULL,
    tenant_id   VARCHAR(64)     NOT NULL,
    number      BIGINT UNSIGNED NOT NULL,
    status      VARCHAR(32)     NOT NULL,
    spec_digest VARCHAR(80)     NOT NULL,
    created_at  VARCHAR(40)     NOT NULL,
    body        LONGTEXT        NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    UNIQUE KEY revisions_function_number (function_id, number)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS revision_counters (
    function_id VARCHAR(64)     NOT NULL,
    last_number BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (function_id) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

CREATE TABLE IF NOT EXISTS aliases (
    function_id VARCHAR(64)     NOT NULL,
    name        VARCHAR(64)     NOT NULL,
    tenant_id   VARCHAR(64)     NOT NULL,
    revision_id VARCHAR(64)     NOT NULL,
    generation  BIGINT UNSIGNED NOT NULL,
    updated_at  VARCHAR(40)     NOT NULL,
    body        LONGTEXT        NOT NULL,
    PRIMARY KEY (function_id, name) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

CREATE TABLE IF NOT EXISTS invocations (
    id           VARCHAR(64) NOT NULL,
    tenant_id    VARCHAR(64) NOT NULL,
    function_id  VARCHAR(64) NOT NULL,
    revision_id  VARCHAR(64) NOT NULL,
    status       VARCHAR(32) NOT NULL,
    terminal     TINYINT     NOT NULL,
    accepted_at  VARCHAR(40) NOT NULL,
    finished_at  VARCHAR(40) NULL,
    input_digest VARCHAR(80) NOT NULL,
    output_kind  VARCHAR(16) NULL,
    body         LONGTEXT    NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY invocations_function_accepted (function_id, accepted_at, id),
    KEY invocations_terminal (terminal)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;

CREATE TABLE IF NOT EXISTS attempts (
    id             VARCHAR(64)     NOT NULL,
    invocation_id  VARCHAR(64)     NOT NULL,
    tenant_id      VARCHAR(64)     NOT NULL,
    number         BIGINT UNSIGNED NOT NULL,
    environment_id VARCHAR(64)     NOT NULL,
    epoch          BIGINT UNSIGNED NOT NULL,
    status         VARCHAR(32)     NOT NULL,
    terminal       TINYINT         NOT NULL,
    body           LONGTEXT        NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY attempts_invocation_number (invocation_id, number),
    KEY attempts_terminal (terminal)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;

CREATE TABLE IF NOT EXISTS environments (
    id                        VARCHAR(64)     NOT NULL,
    tenant_id                 VARCHAR(64)     NOT NULL,
    revision_id               VARCHAR(64)     NOT NULL,
    provider                  VARCHAR(32)     NOT NULL,
    state                     VARCHAR(32)     NOT NULL,
    terminal                  TINYINT         NOT NULL,
    epoch                     BIGINT UNSIGNED NOT NULL,
    execution_role_version    BIGINT UNSIGNED NOT NULL,
    configuration_version     BIGINT UNSIGNED NOT NULL,
    resource_profile_digest   VARCHAR(128)    NOT NULL,
    runtime_profile           VARCHAR(128)    NOT NULL,
    network_policy_version    BIGINT UNSIGNED NOT NULL,
    secret_binding_generation BIGINT UNSIGNED NOT NULL,
    idle_since                VARCHAR(40)     NULL,
    created_at                VARCHAR(40)     NOT NULL,
    body                      LONGTEXT        NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY environments_reuse_key (
        state, tenant_id, revision_id, execution_role_version, configuration_version,
        resource_profile_digest, runtime_profile, network_policy_version,
        secret_binding_generation, id
    ),
    KEY environments_state_idle_since (state, idle_since, id),
    KEY environments_terminal (terminal, id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4;

CREATE TABLE IF NOT EXISTS leases (
    id             VARCHAR(64)     NOT NULL,
    environment_id VARCHAR(64)     NOT NULL,
    attempt_id     VARCHAR(64)     NOT NULL,
    tenant_id      VARCHAR(64)     NOT NULL,
    epoch          BIGINT UNSIGNED NOT NULL,
    deadline       VARCHAR(40)     NOT NULL,
    released       TINYINT         NOT NULL,
    body           LONGTEXT        NOT NULL,
    PRIMARY KEY (id) NONCLUSTERED,
    KEY leases_environment (environment_id),
    KEY leases_released (released)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2;

CREATE TABLE IF NOT EXISTS idempotency (
    tenant_id     VARCHAR(64)  NOT NULL,
    function_id   VARCHAR(64)  NOT NULL,
    idem_key      VARCHAR(256) NOT NULL,
    invocation_id VARCHAR(64)  NOT NULL,
    input_digest  VARCHAR(80)  NOT NULL,
    PRIMARY KEY (tenant_id, function_id, idem_key) CLUSTERED,
    KEY idempotency_invocation (invocation_id)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

CREATE TABLE IF NOT EXISTS artifact_owners (
    digest    VARCHAR(80) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (digest, tenant_id) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;
