-- 002: retention for inline invocation output (expand only).
--
-- Adds a nullable column and its index; no existing column changes meaning,
-- so rows written at schema 1 stay valid at schema 2. Inline outputs that
-- already exist get their expiry from the store on the next open
-- (`backfill_output_expiry`), not from SQL, because the retention period is
-- configuration, not schema.
ALTER TABLE invocations ADD COLUMN output_expires_at VARCHAR(40);
CREATE INDEX invocations_output_expires ON invocations (output_expires_at);
