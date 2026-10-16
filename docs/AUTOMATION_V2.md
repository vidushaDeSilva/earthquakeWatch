# Automated operation and clean development reset

This runbook supersedes routine manual capture/recover/cycle instructions in
earlier sprint guides. Install and merge once: the reviewed configuration requests
the destructive reset automatically on first deployment, then the scheduled
controller owns normal operation. No Mac data collector, Airflow
server, external monitoring service, new paid compute, or Aiven API token is used.

## What changed

The old design waited for Spark in GitHub and failed when its40-minute wait or
32-batch pass ended. That was an orchestration defect. The replacement runs a
short controller every30 minutes at minutes17 and47 UTC. It records intent and
the Databricks run ID in /Shared/earthquake-watch/_automation_state, a small
workspace notebook containing JSON metadata, not executable pipeline logic or
earthquake payloads. A running Spark Job is polled once; the GitHub runner exits.
The next scheduled tick continues automatically. No operator copies run IDs.

The earthquake-dev-handoff-ledger Job is discovered by exact name or created
automatically if absent. Duplicate exact names stop safely. Optional old numeric
ID hints are validated; a genuine not-found falls back to discovery. An ambiguous
create reply is not immediately retried; the next tick discovers the created Job.
The existing earthquake-dev-capture-ledger is also discovered for writer shutdown.
The handoff Job becomes one Serverless notebook
task, rather than seven separately initialized task sessions. The controller
assembles repository notebook includes into two self-contained source notebooks
at deployment, so nested %run widget defaults cannot mask Job/reset parameters.
The workspace receives automated_setup and automated_pipeline only. All code
remains reviewable as ordinary repository files. Code changes deploy only after
an active Job terminates; a code fingerprint avoids redeploying for docs-only commits.

Normal state sequence:

1. Set up/import the lakehouse automatically, then freeze feeds in the Volume.
2. Drain existing frozen capture/Kafka work and submit a SEED processing pass.
3. On Job completion, inspect its immutable report; automatically continue if
   capture/batch work remains. Do not collect a new cycle ahead of this backlog.
4. Fetch the exported tracked-ID checks and refresh the day feed, drain, verify.
5. Gold auto publishes only if every existing coverage/freshness/quality gate
   passes. Otherwise it remains a preview; no bypass or manual promotion is needed.
6. A healthy publication gets a daily snapshot and bounded plan/revalidate/apply
   retention in the same serialized session. Unhealthy data cannot authorize cleanup.
7. After completion, wait one hour before beginning the next observation cycle.
   The small month feed is fetched daily, while day/significant observations
   refresh each cycle. These are scheduled snapshots, not continuous streaming.

Every launch token is saved BEFORE run-now. A lost reply or runner termination
reuses that token, which resolves to the same run. On terminal failure only, retry
at most3 attempts with saved backoff. Pending work itself is a successful bounded
pass, not an exception. Three non-progressing passes,32 passes/cycle, unresolved
quarantine, alias conflicts, corrupt evidence and other unsafe outcomes stop
publication and surface a native GitHub failure. They are not silently ignored.

Known quota-labelled Job failures wait24 hours automatically; long HTTP
Retry-After values are saved across ticks. An expired Kafka history that cannot
be proved, a permission denial, invalid deployment or persistent real code/data
error still needs investigation. Automation cannot safely fabricate missing data
or grant itself missing privileges. A committed code fix is recognized on later
ticks; no reset is needed for a normal code fix.
Repeated connection/service failures open a one-hour circuit and then retry
automatically; they do not require someone to restart scripts.

## Install the changed files

The complete ZIP contains earthquake-watch/. The alternative changed-files ZIP
contains earthquake-automation-changes/repo-update/, FILE_MANIFEST.md (NEW/REPLACE)
and SHA256SUMS.txt. Choose one installation path, not both. This snapshot is based
on the project files available in this session, not unseen custom GitHub edits.
Review overlapping local custom edits before copying.

On the Mac, assuming the ZIP is extracted in Downloads:

```bash
cd "/Users/vidushadesilva/Desktop/projects/earthquake watch"
git switch -c automated-pipeline
cp -R "$HOME/Downloads/earthquake-watch/." .
git diff --stat
```

Commit only the listed project files, push, and merge into the DEFAULT branch.
The workflow push trigger supports main/master; for another default branch,
update that branch filter. The schedule itself always runs on the default branch.
Keep all files from earlier sprints: automatic deployment expands those notebooks.
The complete snapshot already includes them. Tests run automatically before
cloud mutation; a local Python environment is not required for installation.

For the changed-files alternative, replace only the cp command with:

```bash
cp -R "$HOME/Downloads/earthquake-automation-changes/repo-update/." .
```

No manual notebook import, setup execution, new warehouse, job creation, table
truncation, consumer-offset reset or script sequence is required after this merge.
The earlier six manual workflows become inactive notices, so they cannot compete
with the controller. quality.yml still runs offline checks. Direct notebook runs
and another repository using these same credentials are outside the writer lock.

## Existing configuration

Keep these existing repository secrets:

- DATABRICKS_TOKEN
- AIVEN_KAFKA_USERNAME
- AIVEN_KAFKA_PASSWORD
- AIVEN_KAFKA_CA_PEM

Keep these existing repository variables:

- DATABRICKS_HOST
- AIVEN_BOOTSTRAP_SERVERS

EQ_HANDOFF_LEDGER_JOB_ID and EQ_CAPTURE_LEDGER_JOB_ID are optional compatibility
hints, not required configuration. Blank, malformed, deleted or unrelated hints
fall back to exact-name discovery and cannot become mutation targets. Earlier
classic/multi-task versions of the exact handoff Job are migrated to the single
Serverless task automatically. No one copies Job IDs as part of setup. Old
generation/group variables are no longer required.

The workflow fixes the dev Volume/topic explicitly. After reset, generation and
consumer group come from the journal, so no one manually updates these values.
EQ_AUTOMATION_ENABLED=false disables future scheduled ticks for maintenance.
It does not cancel an already active Databricks run.

The existing Databricks token needs manage access to the named project Jobs,
write access to /Shared/earthquake-watch, and ownership/manage rights for the
project tables/Volumes. The Kafka identity needs describe/create/delete on the
two named project topics and delete on the project consumer groups. Permission
denials fail visibly; the code does not change accounts or bypass ACLs.

## Automatic first-deployment reset

config/automation.json carries initial_reset_id=clean-20261016 and the reviewed
RESET_EARTHQUAKE_DEV confirmation. The first default-branch push starts this
requested reset automatically. The shared concurrency group cancels the older
GitHub writer; the controller stops its Databricks writers before deletion.
No Run workflow click or manual database setup is required.

The cloud journal saves bootstrap_reset_completed independently of last_reset_id.
That prevents normal pushes from deleting data again, even after a later explicit
reset uses another ID. An incomplete saved reset finishes before any new initial
configuration takes effect. Keep this journal: deleting it can re-arm the reset.
Changing initial_reset_id deliberately requests another destructive clean baseline.

## Exceptional later reset request

Only if you intentionally want a future wipe, the same workflow also supports
GitHub → Actions → Earthquake automated pipeline → Run workflow:

```text
mode: reset
reset_id: another-deliberate-reset
confirmation: RESET_EARTHQUAKE_DEV
```

This is a deliberate data deletion, not a routine recovery action. Only request
it if you no longer need the existing earthquake development history. Do not
delete the state notebook or issue a different reset ID while a reset is active.
Re-running the SAME ID resumes its journaled work; a completed reset ID will not
erase the newly collected data again. A new ID requests a new destructive reset.

The workflow cancels the prior GitHub ingestion run, then the controller:

1. Pauses old schedules on the configured earthquake-dev Jobs, requests exact
   run cancellations, and waits for actual termination before deleting data.
2. Deletes earthquake_events_dev, observes it absent, recreates it with its
   previous partition/replication counts, delete cleanup,72-hour/256MiB-per-
   partition retention bounds, and a fresh logical stream generation/group.
3. Deletes earthquake_compat_smoke if present and inactive groups matching
   earthquake-dev-landing-* or earthquake-compat-*. It does not delete the Aiven
   service, users, credentials, internal Kafka topics or unrelated topics.
4. Runs the scoped Databricks reset and setup as a Job. Inventory is validated
   BEFORE any Databricks DROP. It drops managed tables/views in earthquake_dev's
   ref/ops/bronze/silver/gold and replay_* schemas, plus the landing Volume.
   It also removes the known earthquake_compat.default.earthquake_compat_probe
   and earthquake_compat.default.smoke_test Volume if present. Unknown schemas,
   default-schema data or external assets stop the reset instead of widening it.
5. Recreates reference seeds, all tables/views/landing directories, configures
   the consolidated pipeline, and begins fresh collection automatically.

The empty catalog/schema containers, source notebooks, Databricks run history
and tiny automation/reset journal are retained. There is no cross-service atomic
reset: if a stage fails, collection remains stopped and the recorded reset resumes
at its stage. Managed objects are logically removed immediately; physical file
purge follows Databricks' lifecycle. A dropped Volume cannot be recovered using
time travel, and Kafka deletion is destructive. No zero-retention vacuum is used.

Reset and first publication take multiple scheduled ticks; you do not need to
click Run workflow between stages. No cloud deletion was executed while building
this package because this session does not have your configured service access.

## What the status means

| Controller status | Meaning and automatic next action |
| --- | --- |
| RESETTING / WAITING_FOR_EXISTING_JOB | Stop writers or advance the exact reset stage on the next tick. |
| WORK_IN_PROGRESS | A known Job is running, or a bounded phase is underway. |
| CONTINUATION_SCHEDULED | Remaining batches/checks will continue without another manual request. |
| RETRY_SCHEDULED | A transient operation or terminal failed Job has saved retry work. |
| SERVICE_COOLDOWN | A repeated connection outage opens a one-hour circuit, then retries automatically. |
| QUOTA_PAUSED | A quota-labelled Job failure defers the next attempt24 hours. |
| COMPLETE | The report proves zero pending work and a gated Gold publication; wait for next cycle. |
| OVERDUE_JOB | Same known Job remains active beyond3 hours; do not launch another writer. |
| HALTED_* | Persistent error, unsafe data, failed proof or no progress; inspect the reported run/issues. |

A green GitHub tick means the controller advanced safely, not that it published
new data. COMPLETE is the full-cycle result. GitHub's step summary shows the
phase, active run ID, pending counts, issues and safe error class. Databricks
ops tables and v_pipeline_health remain the data-level audit/freshness evidence.

Optional read-only checks after the first COMPLETE:

```sql
SELECT state, COUNT(*) FROM earthquake_dev.ops.captures GROUP BY state;
SELECT data_stage, COUNT(*) FROM earthquake_dev.ops.batches GROUP BY data_stage;
SELECT checked_at, status, issues, pending_captures
FROM earthquake_dev.ops.reconciliations ORDER BY checked_at DESC LIMIT 1;
SELECT * FROM earthquake_dev.ops.serving_pointer;
SELECT * FROM earthquake_dev.ops.v_pipeline_health;
```

Expect captures ACCOUNTED, batches SILVER_COMMITTED, latest reconciliation
SUCCESS/[]/0 and a pointer to a READY publication. Querying mid-cycle can
legitimately show pending captures. Do not change ledger states manually.

## Free-tier and capacity limits

All data work stays in Aiven/Databricks and in transient runner memory. Only
repository code and a small nonsecret GitHub summary use runner files. Standard
Linux Actions runners are free for public repositories; private repositories
have included-minute limits. A thirty-minute controller schedule has about48
ticks/day before pushes/tests, so a private repo's allowance must be checked.
No paid compute or billing configuration is enabled by this code.

GitHub schedules can be delayed/dropped and public-repo schedules can be disabled
after60 days without activity. Free Databricks compute is quota-limited with no
SLA. The code automates safe progress and backoff; it cannot promise uninterrupted
operation, exact wall-clock collection times or freshness through a provider outage.
Measure actual Serverless usage before increasing cadence. Source/metadata
inventory bounds from Sprints7–8 still fail visibly rather than truncate data;
this is a bounded portfolio implementation, not an unbounded production service.
Automatic cleanup uses the existing90d/365d/2y policies and preserves identity
watermarks/audits. Safety VACUUM and rollback remain explicit exceptional actions.

## Documentation checked before implementation

- [Jobs list](https://docs.databricks.com/api/jobs/v2/list-jobs): exact
  case-insensitive name filtering and paginated Job discovery.
- [Serverless Jobs compute](https://docs.databricks.com/aws/en/jobs/compute):
  supported notebook tasks default to managed Serverless without classic clusters.
- [Databricks Jobs run-now](https://docs.databricks.com/api/jobs/v2/run-now):
  a saved idempotency token resolves an uncertain launch to the same run.
- [Jobs run API](https://docs.databricks.com/api/jobs/v2/run): active run listing,
  pagination, state polling, task retry and timeout configuration.
- [Workspace import](https://docs.databricks.com/api/workspace-objects/v1/notebook):
  SOURCE/PYTHON imports, base64 content and controlled overwrite.
- [Widgets](https://docs.databricks.com/aws/en/notebooks/widgets): nested %run
  defaults; deployment removes nested execution to preserve Job parameter scope.
- [kafka-python2.3.2 admin](https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaAdminClient.html):
  topic administration and checked consumer-group deletion results.
- [DROP VOLUME](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-drop-volume):
  managed physical-deletion lifecycle and unrecoverable dropped Volumes.
- [GitHub schedule](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows):
  default-branch scheduling and delay/inactivity limits.
- [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
  and [Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

Offline acceptance exercises the controller's real transitions and existing
data engines. Real Aiven deletion/ACLs, workspace import, Serverless consolidated
execution, managed reset, automatic cleanup and free-tier usage still require
the first cloud run. No production/cloud validation is claimed from mocks.

Delivery verification:233 offline tests pass with no skips;45 Python files pass
syntax/purpose/docstrings; eight workflow YAML files parse; both assembled
notebooks compile and contain no nested %run execution.
