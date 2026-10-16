# Sprints 7–8: recovery, source checks, retention and rollback

Implementation handoff, 15 October 2026. Apply over the delivered Sprints 1–6.
The user's current remote repository was not inspected; compare complete
replacement files with any local changes before copying. The assistant has not
connected to the user's Databricks workspace, Aiven service or repository.

## Starting evidence

User screenshots show 24 SILVER_COMMITTED batches, 1,887 current events, 1,889
Silver history versions, zero current withdrawals/conflicts, and two old
usgs-m2p5-day captures still CAPTURED. Gold is BUILDING/PARTIAL with pending
captures and a truncated stale/unverified-source issue. Those are the inputs to
recovery, not evidence that recovery or Gold activation has already happened.

## Design decisions

- Recover captures by their exact capture_id. Frozen responses and original
  fetched_at survive retries. Dispatch receipts are reused; unacknowledged sends
  can duplicate Kafka deliveries, with stable source item/version identity.
- A valid ACCOUNTED receipt is the normal completion checkpoint. No manual
  status update, deletion, refetch or automatic declaration of redundancy closes
  the old capture gap. A missing/corrupt frozen response fails visibly.
- All external HTTP stays in the ephemeral Actions runner. Databricks reads
  verified Volume evidence, and continues using the same serialized Job.
- A cycle first drains previous frozen work, prepares tracked event IDs, freezes
  fresh day/month/significant feeds and bounded event-ID lookups, then dispatches,
  lands and invokes the same Job for verification. Preview is always forced.
- Tracked IDs include current eligible events and the currently served watchlist.
  Check at most 64 IDs per cycle; require an accounted lookup within 24 hours.
  A 404/204/409 is unresolved, never an invented deletion. Explicit deleted
  source features use the existing timestamp/alias validation in Bronze/Silver.
- Catalog wrappers preserve exact original HTTP bytes and a frozen response
  checkpoint. A receipt-time wrapper timestamp is explicitly labeled
  RESPONSE_RECEIVED_NOT_SOURCE_GENERATION and never replaces feed freshness.
- API calls are paced at one per second, at most 80 catalog calls per invocation,
  plus existing timeout/retry/jitter/Retry-After behavior. No official USGS rate
  quota is assumed. Catalog responses are bounded to eight MiB; source captures
  retain their existing 16-MiB bound.
- Backfill accepts 1–7 complete UTC days, at most one year old. Use M2.5+ catalog
  queries with included deletions, before/after counts and unique IDs. Split dense
  slices rather than offset-page a moving catalog. Inclusive USGS end times are
  reduced one millisecond to implement half-open project windows. Count agreement
  is not a transactional snapshot of the changing USGS catalog.
- Historical Gold daily rows are computed from the pinned retained Silver state
  for fully covered catalog dates, labeled BOUNDED_CATALOG_BACKFILL. Existing
  30-day hour/day windows are rebuilt as before. A backfill cannot recreate an
  unobserved transient historical source revision, and the magnitude predicate
  cannot rediscover every event demoted below 2.5; targeted known-ID checks are
  separate. No complete worldwide catalog claim is added.
- An expired Kafka offset is never automatically reset. gap-plan freezes a
  reviewed broker range and pending source IDs. repair-gap validates that range,
  resets only that checkpoint and republishes frozen sources under a stable gap
  generation. It certifies selected source-capture scope, not every physical
  record that might once have existed in Kafka. Topic recreation/end regression
  requires a new stream generation and separate investigation.
- Completed Kafka landing bytes may expire after 30 days, frozen source responses
  after 90 days. Keep ready/selection manifests, hashes and completion receipts.
  Retirement receipts distinguish intentional expiry from missing evidence.
  Current source versions and stable identity metadata can outlive ordinary age
  limits as explicit dependency holds; do not claim every raw byte is gone at day90.
- Bronze versions/deliveries: 90 days, subject to current/recent-source holds.
  Resolved quarantine: 30 days after resolution; open quarantine is held.
  Silver history: 365 days, preserving current selected versions. Current Silver
  rows and aliases remain ordering/identity watermarks: do not delete them just
  because they are one year old and later resurrect an older replay. This is an
  explicit refinement of the initial one-year current-row proposal.
- Runs/attempt metadata: one year where no retained source/batch/release depends
  on it; compact capture/batch ledgers and activation audits remain dependency
  holds. Current/previous releases, latest daily revisions and the newest
  designated daily release per UTC day are protected, with a two-year archive
  target. Recent candidates are held seven days. Other old output can expire.
- Cleanup is plan then apply: exact IDs/paths/checksums, maximum 32 actions per
  plan and 1,000 keys per table-delete action, 24-hour plan expiry, immediate
  revalidation, intent audit, independently atomic writes and readback. A changed
  row/file, pending input, open rejection or active data hold blocks deletion.
  Incomplete unselected capture/batch payloads may be removed after seven days,
  one known file at a time, with no recursive directory deletion.
- Gold daily snapshots already exist from Sprint 6. Sprint 8 protects a verified
  active release and records its row digests, policy/input pins and table versions
  in a separate manifest. This is protection against pipeline mistakes within
  this workspace, not an independent backup of the Databricks account.
- Rollback verifies all three retained Gold output digests, requires the reviewed
  serving generation and a reason, then atomically changes the serving pointer
  and its excluded_publication_ids array. The public daily-history view also
  excludes the replaced bad publication. A lost reply is recovered by exact
  pointer readback and the persisted rollback intention. Old source freshness
  remains visible; rollback does not make old facts current USGS observations.
- Bronze replay builds isolated managed tables in earthquake_dev.replay_<id>.
  It requires retained raw, alias and observation evidence, rejects conflicts,
  and leaves serving alone. Repeat the same ID only for identical retained input;
  changed input requires a new ID. This is a scoped rebuild, not a whole-year
  recovery guarantee. Inspect/diff before any deliberate production repair.
- Keep seven-day Delta deleted-file retention and 30-day log retention. A retained
  Gold row snapshot does not require an old Silver time-travel version. Unfinished
  old pins must expire before vacuum. Vacuum preview/apply uses default retention
  without disabling safety checks. Never ignore missing files during RESTORE.
- Development inventories stop visibly at 10,000 capture/batch/request metadata
  items, 10,000 current events, 20,000 aliases/raw replay versions, 100,000 selected
  table rows, and 366 historical correction dates. No silent truncation. This is
  not a claim of sustained free-tier throughput: measure/index/paginate and set
  unattended schedules in Sprint 9 before reaching these limits.

## Update your repository

From the repository, with the extracted package under Downloads:

```bash
cd "/Users/vidushadesilva/Desktop/projects/earthquake watch"
git status --short
git switch -c sprint-07-08
cp -R "$HOME/Downloads/earthquake-sprints-07-08/repo-update/." .
source .venv/bin/activate
python -m pip install -r requirements-kafka.txt
PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -v
python scripts/check_code.py
git diff --stat
```

If `.venv` does not exist, run `python3 -m venv .venv` before activating it. Adjust
only the extracted package path if different. No new runtime dependency is
required. Review FILE_MANIFEST.md, commit, push and merge to the default branch
so the new manual workflows appear in GitHub Actions. The overlay contains
complete replacements, not patch fragments. Preserve local custom changes.

## Databricks import and setup

Import every NEW/REPLACE .py notebook in the package into the same folder as the
existing Sprint 4/5/6 libraries. Use Import Source; do not paste one entire file
into a cell. Each %run must occupy its own cell. There are five new notebooks:

1. sprint_07_reconciliation_library
2. sprint_08_lifecycle_library
3. sprint_07_08_setup
4. sprint_07_reconcile
5. sprint_08_maintain

Also replace sprint_02_sync_capture_ledger, sprint_03_sync_handoff_ledger,
sprint_04_bronze_library, sprint_05_silver_library, sprint_06_gold_library and
sprint_05_06_setup with their delivered replacements. These integration files
must be upgraded together; retention must not run against the old scanners.

Run all cells of sprint_07_08_setup first. It creates seven operations tables and
required directories. Then run all cells of the updated sprint_05_06_setup. It
adds the nullable exclusions array to the existing pointer and recreates views;
it preserves pointer identity/generation, tables and business data. A different
existing schema is a migration error, not permission to drop a table.

## Existing Job: seven serial tasks

Keep earthquake-dev-handoff-ledger and its existing Job ID. Set max concurrent
runs to 1 and run all tasks with success dependencies:

capture ledger → handoff ledger → Bronze → Silver → reconcile_sources → Gold → maintain

The new reconcile_sources task uses sprint_07_reconcile and depends on Silver.
Change Gold's dependency to reconcile_sources. The new maintain task uses
sprint_08_maintain and depends on Gold. The seven tasks run serially, so they do
not require seven simultaneous compute tasks. Reuse existing Serverless compute.

Keep normal Bronze/Silver settings: fail_at=none, recheck=false, max_batches=32.
Add these Job defaults and verify notebook widgets receive matching parameters:

| Parameter | Default |
| --- | --- |
| reconcile_mode | verify |
| maintenance_action | none |
| retention_plan_id | empty |
| rollback_publication_id | empty |
| rollback_expected_generation | 0 |
| rollback_reason | empty |
| replay_id | empty |
| maintenance_fail_at | none |

Keep gold_action=preview, blank gold_as_of_utc/retry ID and existing policy and
processing generation. The updated notebooks run only after setup is complete.
No new secret, Kafka topic, Volume, warehouse or independent Job is introduced.

## First: recover the two old captures

GitHub → Actions → Earthquake reconciliation and recovery (Sprint 7) → Run workflow:

- action=recover
- epoch=first-recovery (or leave blank)
- max_captures=16
- other inputs blank

This automatically discovers the pending captures, dispatches frozen bytes,
lands them, and invokes the same Job through Silver/reconciliation/Gold preview.
Look for DISPATCH_VERIFIED, LANDING_COMMITTED, RECONCILIATION_RESULT and
PASS Sprint 7 recovery. Actual printed spacing follows the source code.

Run sprint_07_08_acceptance.sql statements separately. The pending-captures query
should return zero rows, counts should agree per capture, and all batches should
reach SILVER_COMMITTED. The latest reconciliation may still be BLOCKED for
missing tracked checks; Gold can still report stale feeds. A successful recovery
operation is not a published watchlist. If work remains, completed batches are
safe; rerun to make bounded progress. Never manually change the old capture state.

If OFFSET_GAP occurs, preserve the evidence. Run action=gap-plan to obtain
GAP_PLAN_ID. Inspect its plan.json under reconciliation/gaps/<id> in Catalog
Explorer: broker ranges, stream and selected capture IDs. Run repair-gap only
with that exact gap_plan_id. Do not recreate the topic or change stream generation
as an attempted fix. This explicit reset is a recovery action, not routine ingestion.

## Then: fresh source checks and first publication

Run the same Sprint 7 workflow with action=cycle and a new epoch such as
first-fresh-check. This drains old work, prepares IDs, fetches all three feeds,
checks tracked event IDs, then runs the seven-task Job in preview mode. Retrying
one epoch reuses its frozen request responses; use a new epoch for a genuinely
new observation. At most 16 captures are dispatched per pass by default. For a
larger tracked set, repeat recover until accounted; use max_captures up to 32 only
within the 10,000-item drain budget. A terminal incomplete run never authorizes a
partial publication.

Expect latest reconciliation SUCCESS with issues=[] and pending_captures=0,
no open quarantine, all batches Silver, and a new Gold candidate with no blocking
issues. Expand the entire blocking_issues array; do not judge a clipped screenshot.
A persistent404, alias conflict or same-time contradictory source remains an
investigation; no unsafe automatic conflict winner is introduced.

Once preview is reviewed, Databricks → existing Job → Run now with per-run
parameters gold_action=publish, maintenance_action=snapshot, reconcile_mode=verify,
blank Gold retry/as-of IDs and all fault settings none. Do not copy a preview's
old publication ID: publication pins with blocking issues are immutable. Build a
fresh candidate. Expect GOLD_ACTIVE and a daily snapshot manifest; serving pointer
must name that READY publication. Public health still reports real freshness.

## Backfill and late changes

Use action=backfill, a new epoch and complete past UTC-day boundaries, for example
2026-10-10T00:00:00Z through2026-10-12T00:00:00Z. This retrieves present USGS
catalog state for that origin-time range. It does not recreate the feed as it
looked on those dates. The workflow produces preview only; perform a fresh cycle
before publishing if required source checks are stale. A revision to an event's
origin within the 30-day horizon recomputes both old and new hour/day buckets.
Historical daily corrections are limited to verified covered catalog days and
retained Silver state. A failed slice or partial source scope cannot be hidden.

## Retention: inspect, then apply

Start with Actions → Earthquake lifecycle and rollback (Sprint 8), action=plan.
Inspect ops.maintenance_plans and open manifest_uri in Catalog Explorer. Review
exact paths/IDs/hashes, row counts and dependency holds. New project data normally
has no eligible 30/90-day deletion targets. Do not change real data timestamps to
force a cloud test; offline synthetic fixtures test old data.

Then action=apply with that exact plan_id within 24 hours. Other maintenance
inputs stay blank. Apply revalidates content and eligibility; active data holds,
pending captures/batches and open quarantine block it. Audit rows are in
ops.maintenance_actions. Rerun the same plan after an interruption to resolve
uncertain delete acknowledgements. Generate another plan for the next bounded
batch. Keep maintenance_action=none in ordinary ingestion defaults; Sprint 9 will
wire a measured daily schedule after these cloud gates pass.

Action=vacuum_preview lists default-safe Delta vacuum candidates. Review table
retention and pin age before action=vacuum. Do not turn off safety checks, delete
managed-table files directly, or use a zero-hour retention interval. Managed
predictive optimization may already vacuum according to table properties.

## Rollback and replay drills

Rollback needs two real retained READY releases. Save the known-good target ID
and current pointer_generation from ops.serving_pointer. Run the Sprint 8 workflow
with action=rollback, that target_publication_id, exact expected_generation and a
short reason. It verifies all Gold output hashes and changes the single pointer;
the replaced bad release is excluded from public daily history in the same pointer
transaction. Generation advances, data as-of stays historical, and health may
say DEGRADED. Repeat the same target/generation/reason after a lost reply; it must
not increment twice. A superseded rollback cannot overwrite a newer release.

For a Bronze replay drill, action=replay and replay_id=demo1 creates
`earthquake_dev.replay_demo1.event_versions` and `events_current`. Compare stable
business fields with the live Silver subset whose source/observation evidence is
still retained. No automatic production replacement occurs. Use a new replay ID
if retained input changed. Replay requires an explicit repair plan before any
future merge of corrected results into live state.

For table-level RESTORE investigation, first point serving at a verified release,
stop ingestion launches, record DESCRIBE HISTORY versions and prove every needed
file remains readable. A RESTORE affects one table and creates a new change; it
cannot atomically restore several tables or resurrect vacuumed files. Reconcile
all ledgers/receipts and rebuild Gold afterward. The package implements pointer
rollback and isolated replay; it does not issue an automatic multi-table RESTORE.

## Completion gates and limits

Offline implementation/verification and actual cloud acceptance are distinct.
Cloud acceptance requires: both pending captures accounted; fresh tracked/feed
checks; a successful fresh READY activation; repeated recovery without new
business versions; verified historical slice; inspected retention plan; a
synthetic/dev deletion interruption; known-good pointer rollback with audit
recovery; isolated Bronze replay comparison; and measured free-tier usage.
Do not claim7/8 live-complete from setup success alone. Current alias/current/raw
watermarks, unresolved request incidents and compact durable ledgers intentionally
remain holds instead of being deleted to meet an arbitrary age. Inventory bounds
and work limits fail visibly; sustained scheduling/quota validation remainsSprint 9.

## Primary documentation consulted before implementation

- USGS FDSN catalog API: https://earthquake.usgs.gov/fdsnws/event/1/
  Event-ID lookup, include-deleted behavior, inclusive time boundaries, count,
  offset semantics and 20,000-result service limit. No historical snapshot guarantee.
- Databricks Jobs run-now: https://docs.databricks.com/api/jobs/v2/run-now
  Existing Job ID, string job_parameters and idempotency_token reuse.
- Databricks table history: https://docs.databricks.com/aws/en/tables/history
  Time travel needs both log and files; RESTORE is a new data-changing operation.
- Databricks vacuum: https://docs.databricks.com/aws/en/tables/operations/vacuum
  DRY RUN, seven-day default and managed maintenance behavior.
- Databricks notebook workflows: https://docs.databricks.com/aws/en/notebooks/notebook-workflows
  %run must occupy its own cell; import source notebooks with cell boundaries.


## Verification performed for this delivery

- 195 offline tests pass, including all141 preceding tests and54 new tests.
  The pinned kafka-python2.3.2 client signature tests ran without skips.
- Python syntax and explanatory docstrings pass for38 owned Python files.
- Local PySpark3.5.3 accepts25 project schemas, including all seven new operations
  schemas and the pointer exclusions array. Four public/health views and18
  acceptance queries were analyzed/executed against synthetic empty temporary
  views, with three-part names translated only in the validation harness.
- Five constant integration SQL statements parse in Databricks dialect; all
  workflow YAML parses. No new PySpark/sqlglot/YAML runtime dependency was added.
- The official USGS count example was directly checked: format=geojson returns
  {"count":10208,"maxAllowed":20000}; the parser supports that object as well as
  the plain integer count format. This was a public API contract check, not a
  user pipeline run.
- Managed Delta MERGE/DELETE/VACUUM, actual Volume retirement, Serverless deployment,
  Aiven offset repair, live capture recovery, publish/rollback and quota usage
  remain cloud acceptance gates. Passing offline fixtures does not establish them.
