-- 003: dispatchers, slot lease ownership and expiry, fencing, idempotency
-- expiry (PLT-4631). Expand only: new table, nullable columns or columns with
-- a constant default, and indexes. Rows written at schema 2 have no owner and
-- are settled by the restart rules when a store opens, exactly as before.

-- One row per gateway process incarnation.
CREATE TABLE dispatchers (
    id               VARCHAR(64)  NOT NULL PRIMARY KEY,
    instance         VARCHAR(256) NOT NULL,
    hostname         VARCHAR(256) NOT NULL,
    pid              BIGINT       NOT NULL,
    started_at       VARCHAR(40)  NOT NULL,
    lease_expires_at VARCHAR(40)  NOT NULL,
    stopped_at       VARCHAR(40),
    reclaimed_at     VARCHAR(40),
    body             TEXT         NOT NULL
);

-- The dispatcher that accepted the invocation.
ALTER TABLE invocations ADD COLUMN owner_id VARCHAR(64);
CREATE INDEX invocations_owner_terminal ON invocations (owner_id, terminal);

-- The dispatcher that created the environment (holds its session), and
-- whether it was fenced after its owner lost its lease.
ALTER TABLE environments ADD COLUMN owner_id VARCHAR(64);
ALTER TABLE environments ADD COLUMN fenced SMALLINT NOT NULL DEFAULT 0;
CREATE INDEX environments_owner_terminal ON environments (owner_id, terminal);
CREATE INDEX environments_fenced_terminal ON environments (fenced, terminal);

-- Slot lease owner and ownership expiry (renewed by the owner's heartbeat);
-- `reclaimed` marks a lease another dispatcher released after it expired.
ALTER TABLE leases ADD COLUMN owner_id VARCHAR(64);
ALTER TABLE leases ADD COLUMN expires_at VARCHAR(40);
ALTER TABLE leases ADD COLUMN reclaimed SMALLINT NOT NULL DEFAULT 0;
CREATE INDEX leases_released_expires ON leases (released, expires_at);
CREATE INDEX leases_owner_released ON leases (owner_id, released);
CREATE INDEX leases_environment_released ON leases (environment_id, released);

-- When a binding stops answering (NULL while its invocation is in flight).
-- (tenant_id, function_id, idem_key) stays the primary key: a key is bound
-- at most once across every process that opens the file.
ALTER TABLE idempotency ADD COLUMN expires_at VARCHAR(40);
CREATE INDEX idempotency_expires ON idempotency (expires_at);
