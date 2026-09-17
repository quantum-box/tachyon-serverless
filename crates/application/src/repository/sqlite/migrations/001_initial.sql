-- 001: the control-plane and cell-local tables (docs/adr/0003).
--
-- Portability rules for this directory (so a TiDB/MySQL adapter can reuse the
-- shape, see docs/adr/0003 「実装メモ」):
--   * only CREATE TABLE / CREATE [UNIQUE] INDEX / ALTER TABLE ADD COLUMN;
--   * keys are VARCHAR with an explicit length, never TEXT;
--   * no partial indexes, no WITHOUT ROWID, no AUTOINCREMENT, no triggers;
--   * no foreign keys (the store checks parents inside the write transaction);
--   * timestamps are fixed-width RFC 3339 UTC strings, so they sort as text;
--   * each row keeps the full domain object in `body` (JSON). Columns exist
--     only for lookups, uniqueness, CAS and retention.

CREATE TABLE store_meta (
    meta_key   VARCHAR(64) NOT NULL PRIMARY KEY,
    meta_value TEXT        NOT NULL
);

CREATE TABLE functions (
    id         VARCHAR(64)  NOT NULL PRIMARY KEY,
    tenant_id  VARCHAR(64)  NOT NULL,
    name       VARCHAR(128) NOT NULL,
    -- `name` while the function is live, NULL once deleted: the unique index
    -- below keeps live names unique per tenant and lets deleted names repeat.
    live_name  VARCHAR(128),
    created_at VARCHAR(40)  NOT NULL,
    body       TEXT         NOT NULL
);
CREATE UNIQUE INDEX functions_tenant_live_name ON functions (tenant_id, live_name);
CREATE INDEX functions_tenant_created ON functions (tenant_id, created_at, id);

CREATE TABLE revisions (
    id          VARCHAR(64) NOT NULL PRIMARY KEY,
    function_id VARCHAR(64) NOT NULL,
    tenant_id   VARCHAR(64) NOT NULL,
    number      BIGINT      NOT NULL,
    status      VARCHAR(32) NOT NULL,
    spec_digest VARCHAR(80) NOT NULL,
    created_at  VARCHAR(40) NOT NULL,
    body        TEXT        NOT NULL
);
CREATE UNIQUE INDEX revisions_function_number ON revisions (function_id, number);

CREATE TABLE revision_counters (
    function_id VARCHAR(64) NOT NULL PRIMARY KEY,
    last_number BIGINT      NOT NULL
);

CREATE TABLE aliases (
    function_id VARCHAR(64) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    tenant_id   VARCHAR(64) NOT NULL,
    revision_id VARCHAR(64) NOT NULL,
    generation  BIGINT      NOT NULL,
    updated_at  VARCHAR(40) NOT NULL,
    body        TEXT        NOT NULL,
    PRIMARY KEY (function_id, name)
);

CREATE TABLE invocations (
    id           VARCHAR(64) NOT NULL PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL,
    function_id  VARCHAR(64) NOT NULL,
    revision_id  VARCHAR(64) NOT NULL,
    status       VARCHAR(32) NOT NULL,
    terminal     SMALLINT    NOT NULL,
    accepted_at  VARCHAR(40) NOT NULL,
    finished_at  VARCHAR(40),
    input_digest VARCHAR(80) NOT NULL,
    -- 'inline' | 'digest' | NULL. Only inline output is a body in the ledger.
    output_kind  VARCHAR(16),
    body         TEXT        NOT NULL
);
CREATE INDEX invocations_function_accepted ON invocations (function_id, accepted_at, id);
CREATE INDEX invocations_terminal ON invocations (terminal);

CREATE TABLE attempts (
    id             VARCHAR(64) NOT NULL PRIMARY KEY,
    invocation_id  VARCHAR(64) NOT NULL,
    tenant_id      VARCHAR(64) NOT NULL,
    number         BIGINT      NOT NULL,
    environment_id VARCHAR(64) NOT NULL,
    epoch          BIGINT      NOT NULL,
    status         VARCHAR(32) NOT NULL,
    terminal       SMALLINT    NOT NULL,
    body           TEXT        NOT NULL
);
CREATE INDEX attempts_invocation_number ON attempts (invocation_id, number);
CREATE INDEX attempts_terminal ON attempts (terminal);

CREATE TABLE environments (
    id                        VARCHAR(64)  NOT NULL PRIMARY KEY,
    tenant_id                 VARCHAR(64)  NOT NULL,
    revision_id               VARCHAR(64)  NOT NULL,
    provider                  VARCHAR(32)  NOT NULL,
    state                     VARCHAR(32)  NOT NULL,
    terminal                  SMALLINT     NOT NULL,
    epoch                     BIGINT       NOT NULL,
    execution_role_version    BIGINT       NOT NULL,
    configuration_version     BIGINT       NOT NULL,
    resource_profile_digest   VARCHAR(128) NOT NULL,
    runtime_profile           VARCHAR(128) NOT NULL,
    network_policy_version    BIGINT       NOT NULL,
    secret_binding_generation BIGINT       NOT NULL,
    idle_since                VARCHAR(40),
    created_at                VARCHAR(40)  NOT NULL,
    body                      TEXT         NOT NULL
);
-- claim_for_reuse: state = 'idle' plus all eight reuse-key fields, oldest id.
CREATE INDEX environments_reuse_key ON environments (
    state, tenant_id, revision_id, execution_role_version, configuration_version,
    resource_profile_digest, runtime_profile, network_policy_version,
    secret_binding_generation, id
);
-- list_idle / pool caps: state = 'idle' ordered by idle_since.
CREATE INDEX environments_state_idle_since ON environments (state, idle_since, id);
CREATE INDEX environments_terminal ON environments (terminal, id);

CREATE TABLE leases (
    id             VARCHAR(64) NOT NULL PRIMARY KEY,
    environment_id VARCHAR(64) NOT NULL,
    attempt_id     VARCHAR(64) NOT NULL,
    tenant_id      VARCHAR(64) NOT NULL,
    epoch          BIGINT      NOT NULL,
    deadline       VARCHAR(40) NOT NULL,
    released       SMALLINT    NOT NULL,
    body           TEXT        NOT NULL
);
CREATE INDEX leases_environment ON leases (environment_id);
CREATE INDEX leases_released ON leases (released);

CREATE TABLE idempotency (
    tenant_id     VARCHAR(64)  NOT NULL,
    function_id   VARCHAR(64)  NOT NULL,
    idem_key      VARCHAR(256) NOT NULL,
    invocation_id VARCHAR(64)  NOT NULL,
    input_digest  VARCHAR(80)  NOT NULL,
    PRIMARY KEY (tenant_id, function_id, idem_key)
);
CREATE INDEX idempotency_invocation ON idempotency (invocation_id);

CREATE TABLE artifact_owners (
    digest    VARCHAR(80) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (digest, tenant_id)
);
