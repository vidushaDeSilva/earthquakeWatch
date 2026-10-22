"""Bounded, parameterized SQL for the Earthquake Watch dashboard.

Only reviewed dashboard views and active, pointer-gated Gold views are queried.
No raw payloads, Volume paths, or user-controlled SQL identifiers are used.
"""

RUNS = """
SELECT * FROM earthquake_dev.ops.dashboard_run_summary
WHERE submitted_at >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY submitted_at DESC, pipeline_run_id DESC
LIMIT 500
"""

CAPTURES = """
SELECT * FROM earthquake_dev.ops.dashboard_captures
WHERE fetched_at >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY fetched_at DESC, capture_id DESC
LIMIT 500
"""

BATCHES = """
SELECT * FROM earthquake_dev.ops.dashboard_landing_batches
WHERE created_at >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY created_at DESC, landing_batch_id DESC
LIMIT 500
"""

DAILY = """
SELECT * FROM earthquake_dev.gold.active_daily_metrics
ORDER BY metric_date DESC
LIMIT 60
"""

WATCHLIST = """
SELECT * FROM earthquake_dev.gold.active_watchlist
ORDER BY rank_priority ASC, significance DESC, magnitude DESC
LIMIT 500
"""

STAGES = """
SELECT * FROM earthquake_dev.ops.dashboard_stage_runs
WHERE pipeline_run_id = ?
ORDER BY started_at ASC, attempt_number ASC
LIMIT 100
"""

CHECKS = """
SELECT * FROM earthquake_dev.ops.dashboard_quality_checks
WHERE pipeline_run_id = ?
ORDER BY checked_at ASC, rule_id ASC
LIMIT 200
"""

CURRENT_PUBLICATION = """
SELECT * FROM earthquake_dev.ops.dashboard_current_publication
LIMIT 2
"""
