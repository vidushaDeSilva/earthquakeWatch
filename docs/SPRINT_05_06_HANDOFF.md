# Sprints 5–6 — Silver events and Gold publication

Implementation handoff, 15 October 2026. This overlay uses the delivered Sprint
1–4 files, not an inspection of the user's current remote repository. Compare
the two replacement files with any local edits before copying. Databricks/Aiven
deployment and cloud acceptance have not been performed by the assistant.

## Scope and known deferred issue

Silver verifies committed Bronze observations, resolves explicit USGS aliases,
keeps typed revision history and a nonregressing current state. Gold pins one
Silver Delta version and impact policy, materializes a 30-day watchlist and UTC
hour/day aggregates, and exposes only checked releases through one pointer.

The user deferred the three historical CAPTURED day responses. No automatic
capture_id backlog dispatcher was added. Their likely origin is source-only
Sprint 2 runs, but per-capture cause is not proven. Preserve original files and
metadata. Preview is the default; PENDING_CAPTURES blocks normal publication.
This delivery does not claim unattended operation. Targeted source checks,
historical recovery and backfill remain Sprint 7; lifecycle and rollback drills
remain Sprint 8; scheduling/quota gates remain outstanding.

## Update the repository

Extract the ZIP. The top-level folder is `earthquake-sprints-05-06`, containing
`repo-update/` and `FILE_MANIFEST.md`. From your existing repository, assuming
the extraction is under Downloads:

```bash
cd "/Users/vidushadesilva/Desktop/projects/earthquake watch"
git status --short
git switch -c sprint-05-06
cp -R "$HOME/Downloads/earthquake-sprints-05-06/repo-update/." .
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-kafka.txt
PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -v
python scripts/check_code.py
git diff --stat
git status --short
```

Change only the source extraction path if you saved the ZIP elsewhere. The `/.`
suffix copies hidden workflow directories too. No environment file, secret,
requirements file or existing ingestion implementation is replaced. Tests use
synthetic in-memory fixtures; no user source data is downloaded to the Mac.

Commit the delivered files after reviewing the manifest, push your branch and
merge it to the default branch to expose the new manual GitHub workflow:

```bash
git add databricks/sprint_05_silver_library.py databricks/sprint_06_gold_library.py
git add databricks/sprint_05_06_setup.py databricks/sprint_05_process_silver.py
git add databricks/sprint_06_publish_gold.py databricks/sprint_05_06_acceptance.sql
git add tests/test_sprint_05_06.py docs/SPRINT_05_06_HANDOFF.md docs/PROJECT_MEMORY.md
git add .github/workflows/quality.yml .github/workflows/silver-gold-recovery.yml
git commit -m "Implement Silver events and pinned Gold publication"
git push -u origin sprint-05-06
```

No new pip dependency is needed by the pipeline. Existing Kafka requirements
remain pinned. The quality workflow now installs that pinned client so its
offline API signature tests run rather than being skipped.

## Import notebooks correctly

In Databricks Workspace, open the SAME folder containing your Sprint 4
notebooks. Use the folder's **Import** action to import these five `.py` source
notebooks from `databricks/`:

1. `sprint_05_silver_library.py`
2. `sprint_06_gold_library.py`
3. `sprint_05_06_setup.py`
4. `sprint_05_process_silver.py`
5. `sprint_06_publish_gold.py`

Keep `sprint_04_bronze_library` in that same folder. Importing preserves
`# COMMAND ----------` cell boundaries. Do not paste a whole source file into
one Python cell: `%run` must execute in a cell by itself. If creating manually,
use a separate cell for EACH `%run ./...` directive and put the Python body in
the following cell. Library files are notebooks, not workspace Python modules.

Run all cells of `sprint_05_06_setup` on Serverless. Expect:

```text
PASS Sprint 5/6 table and view setup; serving pointer preserved
```

Setup creates eight managed Delta tables and four views. It verifies explicit
types, preserves existing objects and the pointer, and never drops tables. A
schema mismatch is an investigation/migration requirement, not permission to
delete existing data. Tables are intentionally unpartitioned at this scale.

## Extend the existing Job

Open `earthquake-dev-handoff-ledger`. Keep its existing ID and max concurrent
runs **1**. Use serial success dependencies:

1. Existing capture-ledger task.
2. Existing handoff-ledger task, depending on task 1.
3. Existing Bronze task, depending on task 2.
4. NEW `process_silver`, notebook `sprint_05_process_silver`, depending on task 3.
5. NEW `publish_gold`, notebook `sprint_06_publish_gold`, depending on task 4.

Use the same Serverless compute selection available in your workspace. No
independent Silver/Gold Job, schedule or second writer is added. Existing
`EQ_HANDOFF_LEDGER_JOB_ID` remains unchanged. Use these Job parameters:

| Parameter | Initial value |
| --- | --- |
| `volume_root` | `/Volumes/earthquake_dev/ops/landing` |
| `bronze_fail_at` | `none` |
| `bronze_recheck` | `false` |
| `bronze_max_batches` | `32` |
| `silver_fail_at` | `none` |
| `silver_recheck` | `false` |
| `silver_max_batches` | `32` |
| `gold_action` | `preview` |
| `gold_as_of_utc` | empty string |
| `gold_retry_publication_id` | empty string |
| `gold_policy_version` | `impact-v1` |
| `gold_processing_generation` | `v1` |
| `gold_fail_at` | `none` |

Add notebook task parameters if your Job does not automatically push matching
Job parameter names to notebook widgets. Verify resolved values in run details.
Blank as-of means the Gold task's current UTC clock; do not copy an old example
date into it. On a retry by publication ID, leave as-of blank: persisted pins win.

Launch this SAME Job using Databricks **Run now**, or GitHub Actions →
**Earthquake Silver and Gold recovery (Sprints 5-6)**. The new workflow uses
existing host/token/Volume/Job variables, shares the ingestion concurrency group,
and polls for up to 2400 seconds. Timeout does not cancel the Databricks run;
inspect that run before launching another. Existing handoff workflows keep their
previous launcher timeout. Measure five-task runtime and free quota.

## Inspect the first run

Expected clean business processing:

```text
PASS Sprint 5 Silver run_id=...
SILVER_COUNTS {"observations":...,"processed_batches":...}
GOLD_PREVIEW publication_id=... blocking_issues=...
PASS Sprint 6 Gold run_id=...
```

Preview success means its outputs are inspectable, not that publication gates
passed. With the deferred captures, expect PENDING_CAPTURES. Stale required feeds
may add other blocking issues. The candidate stays BUILDING, and an initially
empty serving pointer stays NULL. Public views are empty before first activation.

Run `databricks/sprint_05_06_acceptance.sql` in the SQL editor or a SQL notebook.
Use the latest candidate's explicit publication ID for preview queries. Check:

- All existing eligible batches advance to SILVER_COMMITTED and get receipts.
- Alias, version and current keys are unique; current/history lineage matches.
- No unintended Silver conflict; optional impact fields remain NULL.
- Gold eligibility, reason codes, labels, origin times and source links are sensible.
- Each grain's event counts reconcile to unique current in-scope earthquakes.
- Coverage and pending captures are visible; pointer remains unchanged in preview.

Current-event count need not equal 1886 raw revisions or 1931 deliveries. Those
are different grains, and exact counts depend on real source identity/revisions.

## Retry, recheck and failure drills

For routine recovery, run the same Job with recheck=false. A missing Silver
receipt is discovered even when its batch already says SILVER_COMMITTED. For
full Silver verification, temporarily set silver_recheck=true. Record business
row counts/content first; repeat must not create new versions/current events or
advance observation timestamps from the processing clock. Reset to false after.
Rechecking can create new Delta commit versions despite identical business rows;
build a fresh Gold candidate after it, rather than trying to activate an old pin.

Silver fault boundaries (use silver_recheck=true when all inputs are already
completed): after-aliases, after-history, after-quarantine, after-current,
after-silver-ledger, after-silver-receipt. Run one selected boundary; expect a
deliberate failed attempt. Set fail_at=none and rerun the same Job to converge.
Verify no receipt precedes proofs and no duplicate facts appear. Do not alter
source files to simulate corruption in the cloud.

Gold preview fault boundaries: after-building, after-watchlist, after-windows,
after-daily. Obtain the candidate ID from ops.publications after the failure,
then rerun with gold_retry_publication_id=<that exact ID>, gold_as_of_utc blank,
gold_fail_at=none. Verify the same output facts and unchanged pointer. Clear
retry ID afterward. Retry fails visibly if the pinned Silver version expired.

Gold after-ready/after-pointer drills require publish mode AND resolved source
gates. They are deferred while the known CAPTURED backlog is intentionally open.
After those gates pass, use the same candidate ID to recover: pointer readback
resolves uncertain activation and repairs its audit before a later activation.
An older candidate cannot overwrite a newer generation or a newer Silver pin.

## Publication policy and visibility

Default eligibility: nondeleted `event_type='earthquake'`, event_time within
[as_of minus 30 days, as_of), and at least one of significance>600, PAGER
yellow/orange/red, magnitude>=6, ShakeMap MMI>=6, tsunami_flag=1. Missing values
do not match. PAGER gives ESTIMATED_IMPACT; other triggers give POTENTIAL_IMPACT.
Community CDI never substitutes for ShakeMap MMI. Priorities are editorial:
PAGER red/orange/yellow, then tsunami/MMI/magnitude/significance; significance,
magnitude descending and event_key break equal priorities in reader queries.

All watchlist and hour/day rows share one pin/as-of/policy. Hour/day windows are
UTC half-open buckets, include zeros and flag clipped boundary buckets. Each
release rebuilds the bounded 30-day horizon, so a revised origin time corrects
both old and new buckets. Daily history keeps changed-to-zero dates, while older
archive rows remain untouched. Older corrections are Sprint 7 backfill work.

Promotion additionally requires no pending captures, no pending Silver batches,
no open Bronze rejections or Silver conflicts and accounted checks for all three feeds. Day-feed
fetch AND source generation must be within 3 hours; monthly feeds within 36
hours. A source generation over 5 minutes ahead of its fetch is unverified.
Activation requires a release no more than 3 hours old and the same current
Silver version; live coverage is rechecked. These are initial project thresholds,
not USGS availability guarantees. Collection scope remains FEED_SNAPSHOTS_ONLY,
not full catalog completeness; targeted event checks are explicitly deferred.

After recovery/fresh source checks and preview review, run the same Job with
gold_action=publish and a NEW candidate (blank retry ID/as-of). No safety bypass
for pending captures is provided. READY precedes the pointer. Failed output,
failed coverage or failed CAS leaves prior serving active. Health views evaluate
live backlog and freshness even while an older good release remains active.
Daily history exposes activated READY revisions, not abandoned READY candidates.

For dashboard consistency, resolve the pointer once per refresh and query both
snapshot tables with that publication_id; independently rereading two current
views could straddle a release. Gold snapshot facts do not join mutable Silver.

## Limits and engineering notes

- Silver: up to32 batches,16MiB verified landing input,10000 accepted observations
  per task; inherited source cache32MiB/256 captures; alias/history inventories
  bounded at20000 rows. Existing128-batch bound remains. Lower max_batches if
  planning exceeds a byte/source budget; committed prior work is retained.
- Gold:10000 current rows/rows per output release; 30-day windows are bounded.
- Source/delivery bytes live only in memory or Databricks Volume/Delta. No direct
  Spark-to-Kafka connection or external monitoring service is introduced.
- Equivalent preferred IDs at identical semantic state do not create a false
  revision conflict: Silver stores a semantic payload hash independent of ID.
- Historical normalized facts are immutable; selection_status can become
  CONFLICT when new same-time evidence arrives. No automatic conflict resolution
  or normalizer migration is implemented. Conflicted batches remain uncertified.
- No global transaction: each Delta MERGE commits separately. Silver uses proofs
  and immutable receipts; Gold uses immutable output rows and a conditional pointer.
- No deletion, VACUUM, automatic rollback, schedule or retention action is enabled.

## Verification evidence

141 tests passed:101 previous plus40 new; pinned client checks ran without
skips. Syntax/docstrings passed for31 Python files and workflow YAML parsed.
Offline tests include actual producer-compatible input verification, business semantics,
interruptions at six Silver and six Gold boundaries, terminal notebook control
flow, pin expiry and publication safety. Local PySpark3.5.3 checks all eight new
table schemas against generated rows. Four public views and20 acceptance queries
ran against synthetic temporary views with translated namespace names;13 setup
statements parsed in Databricks SQL dialect. These
do not execute managed Delta MERGEs or prove Databricks Serverless compatibility.
Cloud SQL, retry/fault gates and quota measurements remain required.

## Primary documentation checked before implementation

- https://docs.databricks.com/aws/en/delta/merge — pre-deduplicate MERGE input;
  only one updating source row may match a target row.
- https://docs.databricks.com/aws/en/tables/history — versionAsOf requires retained
  data/logs; no latest-table fallback after time-travel expiry.
- https://docs.databricks.com/aws/en/transactions/ — multi-table transaction
  prerequisites are not assumed for this existing workspace.
- https://docs.databricks.com/aws/en/notebooks/notebook-workflows — %run executes
  a notebook inline and must occupy its own cell; sibling paths use ./.
- https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php — source identity,
  origin/update times and distinct nullable impact fields.
