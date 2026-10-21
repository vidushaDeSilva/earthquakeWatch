-- One-time additive Gold publication setup; never drops or resets existing data.
-- The activation audit is separate because a pointer change and a ledger write
-- cannot be one atomic transaction across two Delta tables.
CREATE TABLE IF NOT EXISTS earthquake_dev.ops.pointer_history (
 product_key STRING NOT NULL, pointer_generation BIGINT NOT NULL,
 publication_id STRING NOT NULL, previous_publication_id STRING,
 pipeline_run_id STRING NOT NULL, activated_at TIMESTAMP NOT NULL
) USING DELTA;
