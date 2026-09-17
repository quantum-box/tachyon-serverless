-- 004: generation stamps of the configuration published to data planes
-- (PLT-4636, docs/adr/0007-config-distribution-and-auth-leases.md). Expand
-- only: a new table and an index. The counter lives in store_meta under
-- `config_generation`.
--
-- One row per published key (a function, an alias route, a revision, an
-- authorization grant keyed by a digest of its token, a tenant, the policy).
-- `body` is NULL for a tombstone: the key was published and then removed.
-- Tombstones are kept so that a data plane asking `since = n` learns about
-- the removal whatever `n` is.
CREATE TABLE config_publication (
    entry_key       VARCHAR(512) NOT NULL PRIMARY KEY,
    generation      BIGINT       NOT NULL,
    natural_version BIGINT       NOT NULL,
    digest          VARCHAR(80)  NOT NULL,
    body            TEXT
);
CREATE INDEX config_publication_generation ON config_publication (generation);
