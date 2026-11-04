# Stale capture recovery patch

The dispatcher classifies each verified landing batch using the frozen capture
manifest's `source_generated_at`. A capture outside the Gold three-hour window
routes the entire batch to `RECOVERY`. Bronze and Silver process its original
Kafka offsets; `publish_gold` returns without creating a candidate or changing
the serving pointer. The finalizer records a successful completion report with
`processing_mode=RECOVERY` and `completion_kind=RECOVERED` only when Bronze and
Silver committed and blocking checks passed. The normal `LIVE` Gold gate remains.

## Deployment

1. Keep `EQ_AUTOMATION_ENABLED=false` in GitHub repository variables. Apply the
   patch files at their relative paths in the repository, commit, and push to
   the default branch. Build a wheel with `python -m pip install build` followed
   by `python -m build --wheel`. Install that wheel as a Job library in every
   Databricks task that imports `earthquake_watch`. Upload the changed
   `publish_gold.py` and `finalize_run.py` under a **new immutable release
   directory**. Copy the other Job scripts and the unchanged policy bytes into
   that same release directory, and repoint the Job tasks there. The wheel's
   package version remains 0.2.0; explicitly select the newly built wheel.
2. Add a Databricks job parameter `processing_mode` with default `LIVE`. Append
   `--processing-mode {{job.parameters.processing_mode}}` to the `publish_gold`
   task's script parameters. Keep `finalize_run` on `ALL_DONE`; it reads the
   immutable mode from the dispatch intent. Keep maximum concurrent runs at 1.
3. Change the GitHub `EQ_PROCESSING_REVISION` to the new release identifier only
   after the wheel and scripts are in place. The revision is required because
   processing code changed. Existing rejected candidates remain audit evidence.
4. In GitHub Actions, select `Earthquake Watch` > `Run workflow`, use the
   default branch and `dispatch-only` mode. This is allowed even while the
   scheduled automation variable is false. The step reports one `SUBMITTED`,
   `ACTIVE`, `NEEDS_RECONCILIATION`, or `NO_PENDING_BATCH` action. A `SUBMITTED`
   action means the Databricks run was launched, not that it succeeded. Inspect
   its finalizer report and unchanged serving pointer. Repeat manual runs only
   after the previous Databricks run finishes until `NO_PENDING_BATCH`.
5. Run the GitHub workflow manually with `cycle` mode and verify a fresh capture
   yields a `LIVE` run with an ACTIVE Gold publication. Restore
   `EQ_AUTOMATION_ENABLED=true` only after this live acceptance check passes.

An old unresolved dispatch intent is immutable. If `dispatch-once` reports
`NEEDS_RECONCILIATION`, resolve its finalizer and completion report first.
If it repeatedly receives a Jobs API 404, inspect its saved intent Job ID and
the response error before creating more captures. Do not reset Kafka offsets
or remove rejected publication rows.

## Acceptance checks

For a recovery run, verify `BRONZE` and `SILVER` are `COMMITTED`, all recorded
blocking checks pass, completion reports `SUCCEEDED` and `RECOVERED`, and the
serving pointer generation is unchanged. For a live run, verify a committed
Gold stage, ACTIVE/PASS publication, and an advanced pointer generation.
The old quality-rejected batches remain terminal and are not replayed merely
because this code revision changed.
