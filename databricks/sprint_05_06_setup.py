# Databricks notebook source
"""Create Silver/Gold managed tables and publication-consistent public views.

Import this source as a notebook so %run directives occupy separate cells.
Existing Bronze tables, frozen responses and operational evidence are preserved.
"""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
if "GOLD_SCHEMAS" not in globals():
    raise RuntimeError("Import as a source notebook with separate %run cells, then Run all")
if spark.catalog.tableExists("earthquake_dev.ops.serving_pointer") and "excluded_publication_ids" not in spark.table("earthquake_dev.ops.serving_pointer").columns:
    spark.sql("ALTER TABLE earthquake_dev.ops.serving_pointer ADD COLUMNS (excluded_publication_ids ARRAY<STRING>)")
for name, columns in {**SILVER_SCHEMAS, **GOLD_SCHEMAS}.items():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS earthquake_dev.{name} ({columns}) USING DELTA
        TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 7 days',
                       'delta.logRetentionDuration'='interval 30 days')""")
    expected = spark.createDataFrame([], columns).schema
    actual = spark.table("earthquake_dev." + name).schema
    if {f.name: f.dataType for f in actual} != {f.name: f.dataType for f in expected}:
        raise ValueError("Existing schema differs; do not drop the table: " + name)

spark.sql("""MERGE INTO earthquake_dev.ops.serving_pointer t USING
    (SELECT 'earthquake_watchlist' product_key, CAST(NULL AS STRING) publication_id,
        CAST(NULL AS STRING) previous_publication_id, CAST(0 AS BIGINT) pointer_generation,
        CAST(NULL AS TIMESTAMP) updated_at, CAST(NULL AS STRING) run_id,
        CAST(array() AS ARRAY<STRING>) excluded_publication_ids) s
    ON t.product_key=s.product_key WHEN NOT MATCHED THEN INSERT *""")

spark.sql("""CREATE OR REPLACE VIEW earthquake_dev.ops.v_pipeline_health AS
    WITH latest AS (SELECT *,row_number() OVER(PARTITION BY source_id ORDER BY fetched_at DESC,capture_id DESC) rn
        FROM earthquake_dev.ops.captures WHERE state='ACCOUNTED'),
    captures AS (SELECT
        max(CASE WHEN source_id='usgs-m2p5-day' THEN fetched_at END) day_checked_at,
        max(CASE WHEN source_id='usgs-m2p5-month' THEN fetched_at END) month_checked_at,
        max(CASE WHEN source_id='usgs-significant-month' THEN fetched_at END) significant_checked_at,
        max(CASE WHEN source_id='usgs-m2p5-day' THEN source_generated_at END) day_generated_at,
        max(CASE WHEN source_id='usgs-m2p5-month' THEN source_generated_at END) month_generated_at,
        max(CASE WHEN source_id='usgs-significant-month' THEN source_generated_at END) significant_generated_at
        FROM latest WHERE rn=1),
    pending AS (SELECT count_if(state <> 'ACCOUNTED') pending_captures FROM earthquake_dev.ops.captures),
    batches AS (SELECT count_if(data_stage <> 'SILVER_COMMITTED') pending_batches FROM earthquake_dev.ops.batches),
    conflicts AS (SELECT count_if(stage_name='SILVER') silver_conflicts,
        count_if(stage_name='BRONZE') bronze_rejections FROM earthquake_dev.bronze.quarantine WHERE resolution_status='OPEN')
    SELECT p.product_key,p.publication_id,p.pointer_generation,p.updated_at activated_at,
        u.as_of_utc,u.ready_at,u.coverage_status,u.source_checks_json,c.*,n.pending_captures,b.pending_batches,
        x.silver_conflicts,x.bronze_rejections,
        CASE WHEN p.publication_id IS NULL THEN 'NOT_PUBLISHED'
          WHEN n.pending_captures>0 OR b.pending_batches>0 OR x.silver_conflicts>0 OR x.bronze_rejections>0
            OR c.day_checked_at IS NULL OR c.month_checked_at IS NULL OR c.significant_checked_at IS NULL
            OR c.day_checked_at < current_timestamp()-INTERVAL 3 HOURS
            OR c.month_checked_at < current_timestamp()-INTERVAL 36 HOURS
            OR c.significant_checked_at < current_timestamp()-INTERVAL 36 HOURS
            OR c.day_generated_at IS NULL OR c.month_generated_at IS NULL OR c.significant_generated_at IS NULL
            OR c.day_generated_at < current_timestamp()-INTERVAL 3 HOURS
            OR c.month_generated_at < current_timestamp()-INTERVAL 36 HOURS
            OR c.significant_generated_at < current_timestamp()-INTERVAL 36 HOURS
            OR c.day_generated_at>c.day_checked_at+INTERVAL 5 MINUTES
            OR c.month_generated_at>c.month_checked_at+INTERVAL 5 MINUTES
            OR c.significant_generated_at>c.significant_checked_at+INTERVAL 5 MINUTES
            OR u.as_of_utc < current_timestamp()-INTERVAL 3 HOURS THEN 'DEGRADED'
          ELSE 'FEED_SNAPSHOTS_ONLY' END serving_status
    FROM earthquake_dev.ops.serving_pointer p LEFT JOIN earthquake_dev.ops.publications u
      ON p.publication_id=u.publication_id CROSS JOIN captures c CROSS JOIN pending n CROSS JOIN batches b CROSS JOIN conflicts x
    WHERE p.product_key='earthquake_watchlist'""")
for view, table in (("v_watchlist_current", "watchlist_snapshots"), ("v_windows_current", "window_snapshots")):
    spark.sql(f"""CREATE OR REPLACE VIEW earthquake_dev.gold.{view} AS
        SELECT s.*,h.serving_status,h.activated_at,h.source_checks_json,h.coverage_status
        FROM earthquake_dev.gold.{table} s JOIN earthquake_dev.ops.v_pipeline_health h
          ON s.publication_id=h.publication_id
        JOIN earthquake_dev.ops.publications u ON u.publication_id=s.publication_id AND u.state='READY'""")
spark.sql("""CREATE OR REPLACE VIEW earthquake_dev.gold.v_daily_history AS
    SELECT event_date,publication_id,policy_version,event_count,watchlist_count,max_magnitude,
        pager_yellow_count,pager_orange_count,pager_red_count,as_of_utc,coverage_status,is_partial FROM (
      SELECT d.*, row_number() OVER(PARTITION BY d.event_date ORDER BY d.as_of_utc DESC,d.publication_id DESC) revision_rank
      FROM earthquake_dev.gold.daily_history d JOIN earthquake_dev.ops.publications u
        ON d.publication_id=u.publication_id AND u.state='READY'
      CROSS JOIN earthquake_dev.ops.serving_pointer pointer
      JOIN (SELECT DISTINCT scope_id FROM earthquake_dev.ops.stage_attempts
        WHERE stage_name='PUBLISH_POINTER' AND status='SUCCESS') activated ON activated.scope_id=d.publication_id
      WHERE pointer.product_key='earthquake_watchlist'
        AND NOT array_contains(coalesce(pointer.excluded_publication_ids,CAST(array() AS ARRAY<STRING>)),d.publication_id)
    ) WHERE revision_rank=1""")
print("PASS Sprint 5/6 table and view setup; serving pointer preserved")

