-- 002 (TiDB): retention for inline invocation output (expand only).
-- Mirrors ../../sqlite/migrations/002_output_retention.sql. One ALTER TABLE
-- is one atomic multi-schema change in TiDB, and IF NOT EXISTS makes a
-- re-run after a partial failure a no-op for what already exists. TiDB
-- v8.5 refuses an index on a column added in the same ALTER (ERROR 1072), so
-- columns and their indexes are two statements.

ALTER TABLE invocations
    ADD COLUMN IF NOT EXISTS output_expires_at VARCHAR(40) NULL;

ALTER TABLE invocations
    ADD INDEX IF NOT EXISTS invocations_output_expires (output_expires_at);
