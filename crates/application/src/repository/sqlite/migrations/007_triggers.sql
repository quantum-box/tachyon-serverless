-- 007: cron and signed webhook triggers (PLT-4641, docs/adr/0014). Expand
-- only: three new tables. Nothing existing changes.

-- One row per trigger. `body` is the trigger as JSON (spec, target, status,
-- cursor) and never holds a secret. A webhook secret is kept sealed with
-- AES-256-GCM under `[triggers] secret_key_file` in `secret_sealed`, and is
-- erased when the trigger is deleted.
CREATE TABLE triggers (
    id            VARCHAR(64)  NOT NULL PRIMARY KEY,
    tenant_id     VARCHAR(64)  NOT NULL,
    function_id   VARCHAR(64)  NOT NULL,
    -- 'cron' | 'webhook'
    kind          VARCHAR(16)  NOT NULL,
    -- 'enabled' | 'disabled' | 'deleted'
    status        VARCHAR(16)  NOT NULL,
    generation    BIGINT       NOT NULL,
    -- cron only: the next scheduled time not yet handled (enabled only)
    next_fire_at  VARCHAR(40),
    secret_sealed BLOB,
    created_at    VARCHAR(40)  NOT NULL,
    updated_at    VARCHAR(40)  NOT NULL,
    body          TEXT         NOT NULL
);
CREATE INDEX triggers_function ON triggers (function_id, status);
CREATE INDEX triggers_due ON triggers (kind, status, next_fire_at);

-- One row per fire: `cron:<scheduled time UTC>` or `event:<event id>`. The
-- primary key is the "same scheduled time / same event is never fired twice"
-- guarantee. An accepted fire's row is written in the same transaction as the
-- invocation, its input and its outbox event.
CREATE TABLE trigger_fires (
    trigger_id       VARCHAR(64)  NOT NULL,
    fire_key         VARCHAR(160) NOT NULL,
    tenant_id        VARCHAR(64)  NOT NULL,
    kind             VARCHAR(16)  NOT NULL,
    scheduled_at     VARCHAR(40),
    event_id         VARCHAR(128),
    signature_digest VARCHAR(80),
    invocation_id    VARCHAR(64),
    -- 'accepted' | 'refused'
    outcome          VARCHAR(16)  NOT NULL,
    reason           VARCHAR(512),
    created_at       VARCHAR(40)  NOT NULL,
    PRIMARY KEY (trigger_id, fire_key)
);
-- A signed webhook delivery replayed under another event id is the same
-- delivery.
CREATE UNIQUE INDEX trigger_fires_signature ON trigger_fires (trigger_id, signature_digest)
    WHERE signature_digest IS NOT NULL;
CREATE INDEX trigger_fires_created ON trigger_fires (kind, created_at);

-- The single cron scheduler owner among the gateways on this state.db: a
-- dispatcher id (PLT-4631) and the expiry of its ownership.
CREATE TABLE trigger_scheduler (
    name        VARCHAR(32)  NOT NULL PRIMARY KEY,
    owner_id    VARCHAR(64)  NOT NULL,
    acquired_at VARCHAR(40)  NOT NULL,
    expires_at  VARCHAR(40)  NOT NULL
);
