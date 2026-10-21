# Recovery and scheduled operation

## Deployment order

1. Install the updated package and run the offline suite. Upload the new wheel,
   `register_dispatch.py`, `finalize_run.py`, and the already deployed
   `phase3_process.py` and `publish_gold.py` to Workspace. Keep the exact policy
   file paired with the Gold script. Choose a new immutable processing revision
   whenever any script, wheel or policy bytes change.
2. On the existing single-concurrency Databricks Job, add five Job parameters:
   `batch_id`, `pipeline_run_id`, `revision`, `dispatch_id`, `attempt_no`.
   Configure four Python-script tasks with the same wheel dependency:

   | Task | Dependency | Script parameters | Run condition |
   | --- | --- | --- | --- |
   | `register_dispatch` | none | `--batch-id {{job.parameters.batch_id}} --pipeline-run-id {{job.parameters.pipeline_run_id}} --revision {{job.parameters.revision}} --dispatch-id {{job.parameters.dispatch_id}} --attempt-no {{job.parameters.attempt_no}} --databricks-run-id {{job.run_id}}` | all upstream success |
   | `process_bronze_silver` | register | the three existing `--batch-id`, `--pipeline-run-id`, `--revision` parameters | all upstream success |
   | `publish_gold` | process | the same three parameters plus fixed `--policy-path /Workspace/.../impact_policy_v1.json` | all upstream success |
   | `finalize_run` | publish | the same arguments as register, including `--databricks-run-id {{job.run_id}}` | `ALL_DONE` |

   Set maximum concurrent Job runs to 1 and keep task retries bounded. Do not
   turn on a Databricks schedule: GitHub owns the hourly cadence. The finalizer
   writes `completion/<pipeline_run_id>/report.json` and updates `ops.pipeline_runs`.
   A registration failure may also prevent finalization; use the run output and
   immutable intent to diagnose it.
3. Add repository variables `DATABRICKS_HOST`, `AIVEN_BOOTSTRAP_SERVERS`,
   `EQ_DATABRICKS_JOB_ID`, `EQ_PROCESSING_REVISION`, and
   `EQ_AUTOMATION_ENABLED=false`. Add secrets `DATABRICKS_TOKEN`,
   `AIVEN_KAFKA_USERNAME`, `AIVEN_KAFKA_PASSWORD`, and `AIVEN_KAFKA_CA_PEM`.
   The token identity needs confined Volume read/write/list and permission to
   run and inspect this Job. Databricks Job identity needs Volume read/write for
   completion reports and the reviewed table grants.
4. Test `earthquake-watch dispatch-once` against a verified landing batch while
   the GitHub schedule is disabled. The command writes an immutable dispatch
   intent, invokes the Job once, saves its returned run ID and exits. A second
   call observes an active run once; it does not wait. Inspect the four Job
   tasks and the completion report before enabling the schedule.
5. Enable the GitHub workflow by setting `EQ_AUTOMATION_ENABLED=true` only
   after the manual live capture and recovery drills pass. Its cron is hourly at
   minute 17 UTC and `workflow_dispatch` is available for a bounded manual run.
   It captures once, publishes once, lands one bounded batch, then reconciles or
   submits at most one Databricks run.

## Recovery decisions

- Intent exists, receipt absent: rerun `dispatch-once`; it repeats the *same*
  attempt number and token. Never edit the intent or manufacture a run ID.
- Receipt exists, run active: a later schedule makes one status request and
  exits. Do not manually launch an identical Job.
- Jobs API says terminal, completion report missing: the runner reports
  `NEEDS_RECONCILIATION` and blocks another submission. Inspect the failed
  task and Databricks Ops rows. Repair the finalizer against the same run.
- Report says `FAILED` and `retryable=true`: a later run submits attempt 2 or
  3 using a new token under the original dispatch. Unknown, data, quality,
  code and permission failures are terminal until an operator repairs the
  condition and deploys a new revision.
- Bronze merge succeeded but Ops stage did not commit: rerun the same
  registered attempt through Bronze/Silver. The physical key MERGE and count
  reconciliation repair the stage row; check uniqueness by topic, partition
  and offset before resuming Gold.
- Gold candidate is `REJECTED`: inspect `ops.quality_checks`; the serving
  pointer must still identify the previous valid publication. A fresh capture
  and new revision are needed after correcting stale or invalid evidence.

## Read-only acceptance queries

```sql
SELECT pipeline_run_id, dispatch_id, dispatch_attempt_no, batch_ids,
       processing_revision, run_status, retryable, databricks_run_id
FROM earthquake_dev.ops.pipeline_runs
ORDER BY submitted_at DESC LIMIT 20;

SELECT pipeline_run_id, stage_name, stage_status, output_delta_version
FROM earthquake_dev.ops.stage_runs
ORDER BY started_at DESC LIMIT 30;

SELECT p.product_key, p.active_publication_id, p.pointer_generation,
       pub.silver_delta_version, pub.input_batch_ids
FROM earthquake_dev.ops.serving_pointer p
JOIN earthquake_dev.ops.publications pub
  ON pub.publication_id = p.active_publication_id;

SELECT topic, partition_id, offset, count(*) AS copies
FROM earthquake_dev.bronze.event_deliveries
GROUP BY topic, partition_id, offset HAVING count(*) > 1;

SELECT event_version_id, count(*) AS copies
FROM earthquake_dev.silver.event_versions
GROUP BY event_version_id HAVING count(*) > 1;
```

Run the two duplicate queries after every failure drill. Both must return no
rows. Trace one active publication through its `input_batch_ids` to
`ops.landing_batches.partition_ranges`, `capture_ids`, and `ops.captures`.
Record seven consecutive scheduled cycles and one forced transport and one
forced Databricks failure before declaring the live gate passed.
