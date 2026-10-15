# Sprint 4 — Bronze, quarantine and capture accounting

## Delivery status

Implementation and offline verification are complete. Live Databricks execution,
serverless runtime behavior, compute cost and cloud failure drills remain to be
verified. This overlay was integrated with the delivered Sprint 1–3 files; the
user's current GitHub repository and unseen local changes were not fetched.

## What changes

The existing pipeline now continues:

USGS capture → Aiven Kafka → immutable Volume landing → handoff ledger → Bronze.

The producer and consumer still run on GitHub Actions. Bronze runs inside
Databricks and reads its managed Volume, with no direct Kafka network dependency.
Keep the existing capture and handoff tasks. Add one processing task to the SAME
`earthquake-dev-handoff-ledger` Job so all its runs share max concurrent runs = 1.
Do not create an independent Bronze writer or run its notebook interactively
while the Job is running. Configure automatic task retries to zero during drills;
regular retries/repairs use the same idempotent processing code.

### Tables

| Table | Grain | Purpose |
| --- | --- | --- |
| `earthquake_dev.bronze.event_versions` | One semantic source revision | Raw canonical feature JSON, source version, payload hash and first provenance |
| `earthquake_dev.bronze.deliveries` | One topic lifetime/partition/offset | Every accepted or rejected delivery, even after physical Kafka replay |
| `earthquake_dev.bronze.quarantine` | One delivery/processor revision | Rejection codes and lossless raw value evidence |
| `earthquake_dev.ops.check_results` | One rule/scope/run | Input accounting, source accounting and unknown-field observations |

Existing `ops.batches`, `ops.captures`, `ops.runs` and `ops.stage_attempts` are
updated. Bronze is raw validated history; normalization, current-event selection,
alias resolution and same-time revision conflicts are assigned to Silver/Sprint 5.
A Bronze raw version is not an already-approved public watchlist event.

### Identity and replay

- Consumer replay at the same offsets: same delivery IDs, no new delivery rows.
- Application republish at new offsets: new delivery rows, same source version/item.
- Overlapping feeds: distinct capture/item observations, one shared semantic version.
- A genuine revision changes source-version identity. Same update time with different
  semantic payloads retains both for Silver conflict resolution.
- Unknown fields are retained in frozen source evidence; known semantic projection
  remains unchanged. A schema-observation WARN is recorded rather than inventing a
  semantic revision. The version table retains the FIRST observed raw feature.
- The self-contained Databricks helper mirrors the pinned v1 numeric/hash functions;
  tests compare it with the existing producer rather than changing producer IDs.

### Trust and rejection

Verify sealed manifests, exact file bytes, contiguous offsets, raw key/value/header
base64, message digest and delivery IDs BEFORE interpreting message envelopes.
Damage to this landing evidence fails the Job; it is not treated as a bad earthquake.

Then verify each envelope against the ORIGINAL frozen capture, including ordinal,
source/collection IDs, exact canonical feature, feature checksum, key and revision
ID. Only proven lineage may populate capture/item columns in Bronze deliveries.
A genuine invalid source item is quarantined AND counted as a durable rejection.
Malformed/unknown/forged envelopes are quarantined without trusting their claimed
capture, so they cannot falsely complete it. Storage/authentication failures fail
the Job instead of becoming UNKNOWN_CAPTURE or an empty feed.

Required validation covers feature identity, updated/time types, Point coordinates,
finite numerical values, nullable impact fields and supported alert/tsunami types.
Negative depths and negative magnitudes are valid. Missing impact metrics stay
null. Explicit status `deleted` may omit geometry/time and is retained for Silver.

### Write and completion order

1. Plan up to 32 sealed batches and validate all selected evidence in memory.
2. Deduplicate each MERGE input and check immutable existing keys for conflicts.
3. MERGE versions, then quarantine, then deliveries. Each statement is atomic.
4. Re-read targets and prove every expected accepted/rejected delivery and evidence.
5. Persist batch checks, advance `ops.batches` to BRONZE_COMMITTED, then write/readback
   `receipts/bronze/<batch_id>.json` with overwrite disabled.
6. Count DISTINCT verified source items in Bronze-committed batches. Advance eligible
   `ops.captures` to ACCOUNTED only when its full expected item set is present, then
   write/readback `receipts/captures/<capture_id>.json`.
7. A remaining PUBLISH_ACKED capture makes the notebook fail visibly. Older CAPTURED
   sources stay pending with explicit checks; they were not dispatched. Empty source
   captures require dispatch completion before zero-item ACCOUNTED.

There is no cross-table or Kafka/Delta distributed transaction. Current Databricks
multi-table transactions require catalog commits and compatible compute; this
implementation deliberately uses the existing tested table configuration and
recoverable per-table writes. A mid-stage failure leaves durable prior writes;
rerun converges without deleting them or falsifying completion.

Existing receipts are checked against ledger metadata. Normal runs skip completed
batches; `bronze_recheck=true` re-verifies source/landing bytes and persisted facts
and repairs missing receipts. An immutable receipt with different content is a
conflict, never overwritten. Later SILVER_COMMITTED stages survive Bronze replay.

`bronze_insert_count` is the number of persisted versions first attributed to that
batch, recovered from `event_versions.first_batch_id`; it remains correct when the
first attempt committed versions then failed before updating the batch ledger.
The other counts are per delivery, so valid-delivery count can exceed version count.

## 1. Apply the repository update

Unzip `earthquake-sprint-04.zip`. Enter the existing repository, then run:

```bash
cp -R "$HOME/Downloads/earthquake-sprint-04/repo-update/." .
python3 -m pip install -r requirements-kafka.txt
PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
python3 scripts/check_code.py
git status --short
git add databricks tests docs src/quake_pipeline/jobs.py .github/workflows
git commit -m "Implement Sprint 4 Bronze ingestion and source accounting"
git push
```

Adjust only the extracted package path if your downloads folder differs. Compare
REPLACE files against any of your own changes before copying them. Complete
replacements are included, not patches. Existing smoke resources remain separate.
Expected offline result: **101 tests pass** with the pinned Kafka client installed;
syntax/docstrings pass for **25 Python files**. Kafka API tests can be skipped if
the optional pinned client is not installed, so use the installation command above.

## 2. Import Databricks notebooks

Put these three NEW Python notebooks into the SAME Databricks workspace folder
as your existing Sprint 2 and Sprint 3 notebooks:

1. `sprint_04_setup.py`
2. `sprint_04_bronze_library.py`
3. `sprint_04_process_bronze.py`

Use Import → File and preserve notebook names without the `.py` suffix where
Databricks does so automatically. The processing notebook's second cell must be:

```python
%run ./sprint_04_bronze_library
```

The helper is loaded by the processing notebook; do not add it as a Job task.
Run ONLY `sprint_04_setup` once on serverless compute. Expected:

`PASS Sprint 4 Bronze and quality table setup`

No package installation, Kafka credentials, new Volume or additional catalog is
needed for these notebooks. Managed table DDL does not delete source files.

## 3. Extend the existing Job

Open Workflows/Jobs → `earthquake-dev-handoff-ledger` → Tasks.
Keep max concurrent runs **1**. Add a notebook task:

| Setting | Value |
| --- | --- |
| Task key | `process_bronze` |
| Notebook | `sprint_04_process_bronze` |
| Depends on | Existing handoff-ledger task (which depends on capture-ledger task) |
| Compute | Serverless |
| Automatic retries during acceptance | 0 |

Resulting order:

`sync_capture_ledger → sync_handoff_ledger → process_bronze`

Add these JOB parameters (in Job Details/Edit parameters, not a new schedule):

| Parameter | Value |
| --- | --- |
| `volume_root` | `/Volumes/earthquake_dev/ops/landing` |
| `bronze_fail_at` | `none` |
| `bronze_recheck` | `false` |
| `bronze_max_batches` | `32` |

Keep your actual existing first/second task names. Job parameters automatically
reach notebook widgets. `EQ_HANDOFF_LEDGER_JOB_ID` in GitHub remains this same
existing Job ID. No new Kafka secret or bootstrap setting is needed.
The legacy variable name `EQ_CAPTURE_LEDGER_JOB_ID` inside the launcher is an
implementation detail: the handoff and recovery workflows map the handoff Job ID
to it. The actual Job now executes all three tasks.

The replacement `jobs.py` allows 30 minutes of polling by default, with bounded
HTTP/time budgets. Optional env `EQ_LEDGER_WAIT_SECONDS=60..2400` changes that bound.
A timeout stops waiting; it does not cancel the Databricks run. Inspect it before
launching recovery. Same-request retry reuses its idempotency token; a fresh GitHub
attempt or direct Run now provides a new launch for a failed run.

## 4. Process your existing 22 batches first

With no GitHub workflow/other Job running, choose **Run now** on the SAME Databricks
Job. It syncs source/landing evidence and processes already stored batches. This
path does not contact Kafka, so it still works if old broker messages have expired.
Expected third-task log:

`PASS Sprint 4 Bronze ingestion: processed_batches=22 run_id=...`

The number can differ if your current cloud inventory changed. Existing two old
CAPTURED-only rows shown in your screenshot should remain pending; the three
PUBLISH_ACKED captures should reach ACCOUNTED when all their items are present.
Source counts are per capture, not distinct earthquake counts across feeds.

If more than 32 batches or 16 MiB remain, prior verified work is retained and the
notebook fails with explicit remaining work. Run again with `bronze_recheck=false`
until drained. If PUBLISH_ACKED items are still missing, inspect the capture check:
use the existing Kafka workflow `land`/`recover` when offsets remain valid. An
expired invalid Kafka offset needs controlled Sprint 7 gap resolution; do not
reset the group automatically. Never delete existing cloud evidence to force green.

## 5. Verify cloud acceptance

Run the statements in `databricks/sprint_04_acceptance.sql` one at a time in a SQL
notebook/editor. Check:

- All your processed batches show BRONZE_COMMITTED, with accepted + quarantined = rows.
- The three dispatched captures show ACCOUNTED and accounted = expected.
- All duplicate-key, broken-disposition and count-mismatch queries return zero rows.
- Bronze versions may be fewer than deliveries because feeds/replays overlap.
- Quarantine may be zero for genuine clean source data; that is valid.
- Quality WARN rows for old undispatched sources or unknown source fields are inspected;
  deliberate failure drills remain in history and do not disappear after recovery.

Record the three Bronze table counts. Run the SAME Job again using Run now with
`bronze_recheck=true`, `bronze_fail_at=none`. This deliberately repeats the actual
writes and verification for already processed batches. The three table counts and
first provenance must remain unchanged. Reset `bronze_recheck=false` afterwards.

For actual source republish, use the existing GitHub handoff workflow with the
SAME retained fixed interval and a NEW dispatch generation, e.g. `s4-replay-1`.
Do this only with valid Kafka offsets. It should add deliveries, keep old source
versions deduplicated and leave distinct source-item accounting unchanged. Never
use a new dispatch generation for an ordinary retry.

## 6. Failure drills

Use Run now with different parameters on the SAME existing Job. Set
`bronze_recheck=true`, and test these `bronze_fail_at` values one at a time:

- `after-versions`
- `after-quarantine`
- `after-deliveries`
- `after-batch-ledger`
- `after-batch-receipt`
- `after-capture-ledger`
- `after-capture-receipt`

Each selected run is expected to fail. Then Run now again with `none` and
`bronze_recheck=true`, verify success and unchanged unique fact counts. The offline
suite additionally covers fresh partial writes, malformed/binary/null messages,
valid-but-invalid source items, forged lineage, corruption, overlapping batches,
empty/partial captures, lost receipt PUT replies and notebook failure status.
Rechecking already processed data exercises replay recovery; to demonstrate a
fresh partial write in the cloud, run one drill against newly landed unprocessed
batches before their first successful processing. Do not alter real files by hand.
Reset Job defaults to `none` / `false` after drills.

You can also run the new GitHub workflow **Earthquake Bronze recovery (Sprint 4)**.
It invokes the same three-task Job using existing cloud files and does not depend
on Kafka connectivity. Normal GitHub Kafka `cycle`/`recover` runs automatically
execute Bronze after the Job has been extended, using the same unchanged Job ID.
The GitHub step still has its Sprint 3 name; the Databricks Job now has three tasks.

## Bounds, retention and scheduling

Development safety bounds remain explicit: max 128 sealed batches, max 256 source
captures, 32 batches / 16 MiB planned data per run, max 10000 planned deliveries,
32 MiB in-memory source verification cache, per-file limits inherited from capture
and handoff. This is a bounded portfolio deployment, not an unlimited inventory.
No data is stored locally. No retention deletion/VACUUM is introduced here; Bronze
receipts establish the future deletion gate, while safe cleanup is Sprint 8.
Keep scheduling manual until cloud replay/failure tests pass AND incremental
inventory/retention/backlog controls can sustain growth beyond these bounds. A
working one-off Bronze run alone does not justify continuous scheduling.

## Primary documentation reviewed before implementation (15 October 2026)

- Delta MERGE: https://docs.databricks.com/aws/en/delta/merge — deduplicate source
  rows before MERGE; MERGE alone does not remove duplicates inside its source.
- ACID: https://docs.databricks.com/aws/en/lakehouse/acid — default statement/table
  atomicity; independent table versions unless explicit supported transactions.
- Transactions: https://docs.databricks.com/aws/en/transactions/ — catalog commits
  and compute requirements for multi-table transactions; not assumed here.
- Volumes: https://docs.databricks.com/aws/en/volumes/volume-files — use managed
  `/Volumes/catalog/schema/volume` paths for files, managed tables for tabular data.
- Utilities: https://docs.databricks.com/aws/en/dev-tools/databricks-utils — fs.ls,
  fs.mkdirs and fs.put with overwrite disabled; Spark binaryFile verifies bytes.
- Job parameters: https://docs.databricks.com/aws/en/jobs/job-parameters — automatic
  parameter pushdown to notebook tasks and job-over-task precedence.
- USGS GeoJSON: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php —
  feature/properties/coordinates and source timestamp format.

## Remaining live acceptance gates

Import/setup; extend Job; process existing batches; SQL reconciliation; same-input
recheck; failure/recovery drills; optional same-capture republish; observe serverless
runtime and free-tier quota. No live Databricks or Aiven execution was performed by
the assistant. Sprint 5 consumes Bronze only after these gates have evidence.
