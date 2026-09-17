-- 006: asynchronous invocation inputs and the transactional outbox
-- (PLT-4639, docs/adr/0010). Expand only: two new tables. Nothing existing
-- changes.

-- The input of an asynchronous invocation, written in the same transaction as
-- the invocation row. Small inputs are kept here (`inline_body`, at most
-- `[invoke_async] inline_input_max_bytes`); larger ones live in the object
-- store and are referenced by (object_id, region) plus an `object_refs` row.
CREATE TABLE invocation_inputs (
    invocation_id VARCHAR(64) NOT NULL PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    -- 'inline' | 'object'
    storage       VARCHAR(16) NOT NULL,
    size_bytes    BIGINT      NOT NULL,
    digest        VARCHAR(80) NOT NULL,
    inline_body   BLOB,
    object_id     VARCHAR(64),
    region        VARCHAR(32),
    created_at    VARCHAR(40) NOT NULL
);

-- One event per accepted asynchronous invocation (event_id = invocation id).
-- `payload` is the routing envelope, never the input body. A publisher claims
-- due rows under a lease (claimed_by / claim_expires_at), publishes with
-- message id = event_id and marks the row sent with a CAS on its claim.
CREATE TABLE outbox (
    event_id         VARCHAR(64)  NOT NULL PRIMARY KEY,
    tenant_id        VARCHAR(64)  NOT NULL,
    topic            VARCHAR(64)  NOT NULL,
    payload          TEXT         NOT NULL,
    created_at       VARCHAR(40)  NOT NULL,
    sent             SMALLINT     NOT NULL,
    publish_attempts BIGINT       NOT NULL,
    next_attempt_at  VARCHAR(40)  NOT NULL,
    claimed_by       VARCHAR(64),
    claim_expires_at VARCHAR(40),
    last_error       VARCHAR(512),
    sent_at          VARCHAR(40),
    queue_sequence   BIGINT
);
CREATE INDEX outbox_sent_next ON outbox (sent, next_attempt_at, created_at);
CREATE INDEX outbox_sent_created ON outbox (sent, created_at);
