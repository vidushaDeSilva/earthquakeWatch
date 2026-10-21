# Retention and read-only consumption

The cleanup script defaults to dry-run. It plans Volume capture and
landing files whose Ops lineage proves terminal processing, associated
receipts after 90 days, or orphan data files older than seven days without
a manifest. It protects the active Gold
publication, open quarantine, and unfinished batches. Files named in an
approved plan are rechecked for eligibility and SHA256 immediately before
removal. The routine GitHub workflow never invokes cleanup.

1. Disable the GitHub schedule by setting `EQ_AUTOMATION_ENABLED=false` and
   wait for any active Databricks processing run to finish. Run the standalone
   Databricks Python-script maintenance task for `databricks/maintain_volume.py`
   without arguments. Save the printed `plan_sha256` and inspect its
   `retention/plan-<sha256>.json` in the Volume. No file is deleted.
   Inspect older manual smoke-test parents too. The table maintenance task
   refuses to run while any Ops parent remains nonterminal; reconcile those
   rows against their actual Job and stage evidence before cleanup.
2. Review every path, active pointer lineage, open quarantine and batch stage
   outcome. To apply exactly that plan, run the separate maintenance task with
   `--apply-plan-sha256 <reviewed digest>`. A changed file or eligibility stops
   deletion. If an apply is interrupted, repeat the same digest; already
   removed paths are counted separately.
3. Run the separate `databricks/maintain_tables.py` maintenance task with no
   arguments. Inspect its `retention/tables-<sha256>.json` candidate keys and
   counts. It plans Bronze deliveries older than 90 days, Silver versions
   older than 365 days that no retained Gold window needs, and Gold rows for
   nonactive terminal publications older than 365 days. Apply only after review
   with `--apply-plan-sha256 <reviewed table digest>`. The script checks that
   the pointer generation has not changed. Ops metadata is retained longer to
   preserve audit history. Neither script runs VACUUM or shortens Delta log
   or deleted-file retention.

Run `databricks/consumer_views.sql` once in SQL Editor. Grant your dashboard
identity SELECT on `earthquake_dev.gold.active_watchlist` and
`earthquake_dev.gold.active_daily_metrics` plus the required catalog/schema
usage grants. It does not need write access or access to raw Kafka deliveries.

```sql
SELECT publication_id, usgs_event_id, event_time, magnitude, place,
       selection_reasons, impact_label, rank_priority
FROM earthquake_dev.gold.active_watchlist
ORDER BY rank_priority, significance DESC, magnitude DESC,
         event_time DESC, usgs_event_id ASC;

SELECT publication_id, metric_date, event_count, watchlist_count,
       max_magnitude, coverage_status
FROM earthquake_dev.gold.active_daily_metrics
ORDER BY metric_date DESC;
```

The views resolve the serving pointer at query time, so a rejected candidate
cannot enter their results. Daily buckets at the rolling 24-hour window edges
are marked `PARTIAL_DAY`.
