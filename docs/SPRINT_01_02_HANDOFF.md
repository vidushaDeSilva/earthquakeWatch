# Sprints 1–2 implementation handoff

## Status

Implementation and offline validation are complete. Your cloud deployment and acceptance runs are pending. This delivery is additive to the known smoke-test repository; its current remote contents were not fetched. Existing `src/probe.py`, `requirements.txt`, `README.md` and `.github/workflows/compatibility.yml` are not replaced.

## New design choices

- New source-capture runtime uses only Python 3.12 standard-library modules. The compatibility workflow retains its existing Kafka/Requests dependencies.
- `EQ_VOLUME_ROOT` is separate from the compatibility test's `DATABRICKS_VOLUME_ROOT`. Development captures require `/Volumes/earthquake_dev/ops/landing` so an accidentally copied smoke-test path fails early.
- One selection manifest freezes a collection. Interrupted selection recovers a complete ready capture using paginated Files API listing.
- A successful source capture and a successful Delta ledger import are separate evidence points. The GitHub workflow automatically triggers the ledger Job for a capture action; a ledger failure leaves source evidence safe for retry.
- Sprints 1–2 create only the five initial reference/operations tables. Later-layer schemas exist but their business tables are introduced in later sprints.
- Secrets/URLs/bodies are excluded from request-attempt telemetry. Failure messages contain safe codes and service/status values.

## Documentation reviewed before implementation

| Primary source | Behavior used |
| --- | --- |
| https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php | Feed names, metadata, event and revision timestamps, count semantics |
| https://docs.databricks.com/api/files/v2/create-directory | Idempotent directory creation and Files API paths |
| https://docs.databricks.com/api/files/v2/upload-file | Raw upload bytes, explicit overwrite=false, directory pagination |
| https://docs.databricks.com/api/jobs/v2/run-now | Job parameters and launch idempotency token |
| https://docs.databricks.com/aws/en/volumes/utility-commands | Managed Volume creation |
| https://docs.databricks.com/aws/en/query/formats/json | Explicit-schema JSON reads |
| https://docs.databricks.com/aws/en/tables/constraints | PK/FK limits; use application key checks |
| https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax | Manual inputs, concurrency, permissions |
| https://docs.python.org/3.12/library/urllib.request.html | HTTPS request/response transport and redirect handlers |
| https://docs.python.org/3.12/library/urllib.error.html | HTTP and transport error classification |
| https://docs.python.org/3.12/library/email.utils.html | HTTP-date Retry-After parsing |

## Offline verification

30 automated tests cover genuine empty feeds; response corruption; count mismatch; nonstandard JSON; semantic IDs; UTC and configuration boundaries; 429/5xx retry; 401/certificate terminal failures; request and body budgets; capture reuse; lost PUT ACK; interrupted selection; immutable conflicts; directory pagination after an empty page; job-launch ACK loss; failed ledger jobs.

The syntax/docstring gate checks all 13 owned Python files. Offline tests use an in-memory fake of USGS/Files API; no live earthquake response or credential is stored locally. Databricks notebooks are syntax-checked, not executed against Spark here.

## Apply to your existing repository

1. Extract the downloaded ZIP. Keep the `repo-update` directory structure intact.
2. In a terminal at the root of your existing `earthquake-stack-smoke` repository, run `git status` and preserve any work already in progress.
3. Create a work branch: `git switch -c sprints-01-02`.
4. Copy the **contents** of `repo-update` into that repository root, including its hidden `.github` directory. Merge directories; do not replace the whole `.github` directory or repository. Example:

   ```bash
   cp -R /absolute/path/to/earthquake-sprints-01-02/repo-update/. .
   ```

5. Compare with `FILES_MANIFEST.md`. All delivered files are NEW relative to the known smoke-test baseline. If any already exist and differ in your repository, review that path before replacing it.
6. Run:

   ```bash
   PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -v
   python3 scripts/check_code.py
   ```

7. Review the new files. Stage the exact listed paths, commit, push the branch and merge it after the quality workflow passes. No new pip install is needed for these checks.

## Databricks setup

1. Import `databricks/sprints_01_02_setup.py` as a Python notebook into your personal Workspace folder. Select Serverless and Run all.
2. Expect `PASS Sprint 1 development namespace and reference tables` and Volume `/Volumes/earthquake_dev/ops/landing`.
3. Import `databricks/sprint_02_sync_capture_ledger.py` as a second Python notebook. Do not run it until a capture exists.
4. Create a new Job named `earthquake-dev-capture-ledger`. Its single Serverless notebook task runs the second notebook. Set maximum concurrent runs to 1.
5. Add Job parameter `volume_root=/Volumes/earthquake_dev/ops/landing`, save and copy the numeric Job ID. This is a new Job, separate from the compatibility Job.

## GitHub variables and secrets

Keep your existing `DATABRICKS_HOST` variable and `DATABRICKS_TOKEN` secret. Add repository variables:

| Variable | Value |
| --- | --- |
| `EQ_VOLUME_ROOT` | `/Volumes/earthquake_dev/ops/landing` |
| `EQ_COLLECTION_GENERATION` | `v1` |
| `EQ_CAPTURE_LEDGER_JOB_ID` | Numeric ID of the new capture-ledger Job |

Keep the compatibility test's `DATABRICKS_VOLUME_ROOT`, `DATABRICKS_JOB_ID` and Aiven settings as configured. The new source capture uses the `EQ_` settings; Kafka is not required for this stage.

## Run and acceptance evidence

On the repository default branch, open Actions -> **Earthquake source capture (Sprints 1-2)** -> Run workflow:

1. `action=preflight`, `mode=hourly`, leave window_end blank. Expect `PASS Sprint 1 configuration preflight`.
2. `action=capture`, `mode=hourly`. Expect CAPTURE_VERIFIED followed by `PASS Sprint 2 cloud source capture`, then `PASS Sprint 2 Databricks capture ledger`.
3. Repeat the same capture in the same UTC hour, or explicitly copy the first run's `window_end` from PREFLIGHT_PASS into the second run. Expect the same capture ID and `reused=true`.
4. Run `action=capture`, `mode=reconcile`. Expect two source captures, for the M2.5+ and significant 30-day feeds, and a successful ledger Job.
5. In Databricks SQL run:

   ```sql
   SELECT source_id, capture_id, expected_item_count, state, fetched_at
   FROM earthquake_dev.ops.captures ORDER BY fetched_at DESC;

   SELECT capture_id, count(*) AS copies
   FROM earthquake_dev.ops.captures
   GROUP BY capture_id HAVING count(*) > 1;

   SELECT source_id, source_revision, feed_key FROM earthquake_dev.ref.sources;
   ```

The duplicate query must return zero rows. Capture states should be CAPTURED with accounted_item_count=0: downstream Kafka/Bronze delivery has not started. The reused capture must retain its original fetched_at and expected count. There should be three distinct feed captures after the hourly and reconciliation examples, assuming one collection interval/generation for each.

## Troubleshooting

- Workflow missing: merge the workflow file onto the default branch.
- Preflight missing settings: verify repository Variables versus Secrets and exact names.
- HTTP 401/403: correct token/Volume permission; do not repeatedly retry unchanged credentials.
- Cloud capture passes but ledger fails: inspect the new Databricks Job logs. Source files remain durable. Correct the notebook/configuration, then rerun capture with the first run's window_end to reuse it.
- Ledger checksum/path error: do not overwrite source files. Preserve evidence and inspect the manifest/path normalization.
- Missing selected capture for an old window: historical reconstruction is deliberately rejected until Sprint 7 backfill. Use a current interval or its already captured source evidence.
- A stale-source flag is a data freshness warning, not a newer source observation.

## Exit gate before Sprint 3

Provide evidence of successful setup, successful hourly and reconciliation capture/ledger runs, and same-window replay with the same capture ID and zero duplicate capture rows. Until then mark both sprints implemented/offline-validated, with cloud acceptance pending. Production schedules, Kafka dispatch and retention deletion remain disabled.
