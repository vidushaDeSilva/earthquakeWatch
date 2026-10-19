-- Read-only Phase 0 evidence; operator verifies table and Volume names against bootstrap.sql.
SHOW SCHEMAS IN earthquake_dev;
SHOW TABLES IN earthquake_dev.ops;
SHOW TABLES IN earthquake_dev.bronze;
SHOW TABLES IN earthquake_dev.silver;
SHOW TABLES IN earthquake_dev.gold;
SHOW VOLUMES IN earthquake_dev.ops;
DESCRIBE TABLE earthquake_dev.ops.captures;
DESCRIBE TABLE earthquake_dev.ops.landing_batches;
DESCRIBE TABLE earthquake_dev.ops.pipeline_runs;
DESCRIBE TABLE earthquake_dev.bronze.event_deliveries;
DESCRIBE TABLE earthquake_dev.silver.event_versions;
DESCRIBE TABLE earthquake_dev.silver.events_current;
DESCRIBE TABLE earthquake_dev.gold.watchlist_snapshots;
DESCRIBE TABLE earthquake_dev.gold.daily_metrics;
