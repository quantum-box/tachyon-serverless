-- 008: asynchronous dispatch, retries, dead letters and redrives (PLT-4640,
-- docs/adr/0013). Expand only: three new tables and one column with a
-- constant default. Nothing existing changes meaning.

-- The dispatch state of one asynchronous invocation. Created by the first
-- claim; an invocation without a row has never been claimed (generation 0,
-- no attempt). Ownership of a run is this row's claim, not
-- `invocations.owner_id`: a dispatcher that dies must not settle the
-- invocation (the reclaim rules would make it terminal), only lose its claim.
CREATE TABLE async_dispatch (
    invocation_id    VARCHAR(64) NOT NULL PRIMARY KEY,
    tenant_id        VARCHAR(64) NOT NULL,
    function_id      VARCHAR(64) NOT NULL,
    -- 'running' | 'scheduled' | 'done' | 'dead'
    state            VARCHAR(16) NOT NULL,
    -- Delivery generation: the outbox event of the next try carries
    -- generation + 1, and a delivery of an older generation is stale.
    generation       BIGINT      NOT NULL,
    -- Runs that counted against max_attempts.
    attempts         BIGINT      NOT NULL,
    -- Runs deferred without counting (capacity, retry budget).
    deferrals        BIGINT      NOT NULL,
    claimed_by       VARCHAR(64),
    claim_expires_at VARCHAR(40),
    next_attempt_at  VARCHAR(40),
    first_attempt_at VARCHAR(40),
    last_attempt_at  VARCHAR(40),
    updated_at       VARCHAR(40) NOT NULL,
    -- The last error (JSON InvocationError), if any.
    last_error       TEXT
);
CREATE INDEX async_dispatch_state_claim ON async_dispatch (state, claim_expires_at);

-- A dead-lettered asynchronous invocation, or an event that could not be read
-- at all (poison: no invocation, only the routing tenant and the message id).
-- The input stays where it was (invocation_inputs / object_refs); while an
-- entry is `open`, the object GC keeps a referenced input object.
CREATE TABLE dead_letters (
    id               VARCHAR(64)  NOT NULL PRIMARY KEY,
    tenant_id        VARCHAR(64)  NOT NULL,
    function_id      VARCHAR(64),
    invocation_id    VARCHAR(64),
    -- 'non_retryable' | 'attempts_exhausted' | 'expired' | 'function_deleted'
    -- | 'revision_unavailable' | 'poison'
    reason           VARCHAR(32)  NOT NULL,
    -- 'open' | 'redriven'
    status           VARCHAR(16)  NOT NULL,
    -- `inv:<invocation id>` or, for poison, `msg:<message id>:<sequence>`:
    -- a crash between recording and acking can never record it twice.
    origin_key       VARCHAR(192) NOT NULL,
    created_at       VARCHAR(40)  NOT NULL,
    body             TEXT         NOT NULL
);
CREATE INDEX dead_letters_function_created ON dead_letters (function_id, created_at, id);
CREATE INDEX dead_letters_invocation ON dead_letters (invocation_id);
CREATE UNIQUE INDEX dead_letters_origin ON dead_letters (origin_key);
CREATE INDEX dead_letters_tenant_status ON dead_letters (tenant_id, status);

-- One redrive: the audit record linking a dead letter to the new invocation
-- it created.
CREATE TABLE redrives (
    id                   VARCHAR(64) NOT NULL PRIMARY KEY,
    dead_letter_id       VARCHAR(64) NOT NULL,
    tenant_id            VARCHAR(64) NOT NULL,
    source_invocation_id VARCHAR(64) NOT NULL,
    invocation_id        VARCHAR(64) NOT NULL,
    created_at           VARCHAR(40) NOT NULL,
    body                 TEXT        NOT NULL
);
CREATE INDEX redrives_dead_letter ON redrives (dead_letter_id, created_at);
CREATE INDEX redrives_invocation ON redrives (invocation_id);

-- The delivery generation an outbox event publishes (message id
-- `<invocation id>` for 0, `<invocation id>.g<n>` after a retry was scheduled).
ALTER TABLE outbox ADD COLUMN generation BIGINT NOT NULL DEFAULT 0;
