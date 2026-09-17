-- 005: object references and collection tombstones (PLT-4638, ADR-0008).
-- Expand only: two new tables. Nothing existing changes.

-- Which invocation uses which stored object. An object's tenant and region
-- are recorded with the reference; a reference never crosses the tenant of
-- its invocation (checked in code before the insert, in the same
-- transaction).
CREATE TABLE object_refs (
    object_id     VARCHAR(64) NOT NULL,
    tenant_id     VARCHAR(64) NOT NULL,
    region        VARCHAR(32) NOT NULL,
    invocation_id VARCHAR(64) NOT NULL,
    attached_at   VARCHAR(40) NOT NULL,
    PRIMARY KEY (object_id, invocation_id)
);
CREATE INDEX object_refs_invocation ON object_refs (invocation_id);

-- An object the GC decided to delete. While a tombstone exists no new
-- reference can be attached; it is removed once the files are gone.
CREATE TABLE object_tombstones (
    object_id     VARCHAR(64) NOT NULL PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    region        VARCHAR(32) NOT NULL,
    reason        VARCHAR(16) NOT NULL,
    tombstoned_at VARCHAR(40) NOT NULL
);
