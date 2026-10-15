# Sprint 3 — Kafka publish and durable cloud handoff

Delivered 15 October 2026. Implementation/offline validation complete; deployment
and cloud fault acceptance pending. This overlay assumes the delivered Sprint 1–2
files. The user's current remote repository has not been fetched or modified.

## What changes

- Verified frozen captures act as an outbox. No dispatcher re-fetches a live feed.
- Kafka messages are keyed by USGS event ID; malformed source items use their
  deterministic source-item ID and remain available for Sprint 4 quarantine.
- Producer idempotence and broker ACKs protect client retries within one session.
  A new application session may re-send a chunk whose receipt was never saved.
- Immutable per-chunk ACK receipts avoid ordinary re-publication. A final dispatch
  manifest proves every source item was broker-acknowledged, including empty feeds.
- Consumer uses explicit assignment, one partition at a time, manual commits and
  a retained end-offset snapshot. All ingestion workflows use the SAME repository
  concurrency group, `earthquake-dev-ingestion`. Do not run another consumer or
  separate repository with this group ID: manual assignment has no group rebalance
  to fence competing writers. The concurrency lock is an operational requirement.
- Raw key/value/headers are base64 encoded losslessly in cloud JSONL, including
  null/empty/non-UTF8 values. Data is verified, manifest is written last, both are
  re-read, then the next offset is committed and checked. No raw bytes go to logs.
- Recovery lists sealed manifests independently of committed Kafka offsets. It
  repairs eligible pending commits and leaves EVERY batch LANDED for Sprint 4.
- `ops.batches` and source state PUBLISH_ACKED are imported by a serial Databricks
  Job after independent byte/hash/count/offset checks. ACCOUNTED remains a later
  Bronze gate. A producer ACK alone does not account an event.

## Delivery semantics and time

At-least-once delivery, idempotent cloud evidence and later business-key MERGEs.
No distributed exactly-once transaction. Physical Kafka duplicates after an
ambiguous publication are expected; their distinct delivery IDs must survive.
Source item identity is capture ID + feature ordinal; source version identity is
the existing semantic USGS version formula. The raw delivery ID is a hash of
stream generation, topic, partition and offset.

Collection windows identify retries, not the feed's event-time range. USGS origin
time and revision time remain in the source feature. `captured_at` is the original
durable capture time; replay cannot invent a new source-check freshness timestamp.

The topic accepts one or two partitions, matching current Aiven Free capacity.
Do not change partition count/group/bootstrap in an existing stream generation.
Increment `EQ_STREAM_GENERATION` and choose a new consumer group after a deliberate
topic recreation. Generation changes are operator-controlled; automatic detection
of every topic recreation is not claimed. Topic IDs are not used by this adapter.

## Bounds and retention

| Resource | Bound / behavior |
| --- | --- |
| Topic | Existing `earthquake_events_dev`, 1 or 2 partitions; auto-create disabled |
| Effective Kafka retention | Positive `retention.ms` <=259200000 (72 hours); cleanup.policy exactly delete |
| Source dispatch | 10000 items per capture, 100 items per chunk, 256 KiB per envelope+key |
| Raw landing | At most 100 polled records; split at 4 MiB persisted batch bytes |
| Drain | Snapshot of initial end offsets; max10000 records /600seconds; 10 empty polls on known backlog fail visibly |
| HTTP | 1500 requests and 20minutes per bridge command; existing retry/Retry-After rules |
| Recovery inventory | Up to128 sealed/incomplete batch directories per stream; exceeds bound => visible failure |
| Data storage | In-memory on runner; durable Volume files/Delta only; no payload artifacts/caches |
| Schedule/cleanup | Manual only; no retention deletions until Sprint8; no schedule until Sprint4 gate |

The 128-batch cap makes this a bounded sprint acceptance deployment. Do not enable
continuous collection yet. Later retention and incremental inventories must extend
this scan before steady unattended use. A cap failure preserves existing data.

`retention.bytes` is also printed because byte pressure can delete Kafka data
earlier than its time limit. Free-service quotas and long-run availability are NOT
proven by offline tests. Measure bytes, runtime and actual effective topic settings.

## Cloud paths under /Volumes/earthquake_dev/ops/landing

| Path | Meaning |
| --- | --- |
| `captures/<collection>/<capture>/response.json` | Existing exact source bytes |
| `collections/<collection>/selected.json` | Existing frozen source selection |
| `handoff/<stream>/definition.json` | Immutable topic/group/generation/topology binding |
| `handoff/<stream>/dispatch/<capture>/<dispatch_generation>/chunk-NNNNNN.json` | Acknowledged item range with message hashes and physical broker coordinates |
| `handoff/<stream>/dispatch/<capture>/<dispatch_generation>/manifest.json` | Complete source dispatch, all chunk hashes/counts |
| `handoff/<stream>/batches/<batch>/records.jsonl` | Lossless raw Kafka deliveries |
| `handoff/<stream>/batches/<batch>/manifest.json` | Last-written complete landing seal |
| `handoff/<stream>/runs/<run>/report.json` | Safe stage and HTTP telemetry |

A partial data file without manifest is ignored by recovery and never committed.
No cleanup happens in this sprint. Batch identity hashes stream, partition,
first/last offsets and actual JSONL hash. Original sealed metadata is reused on
retry. Regrouping an unsealed range may leave an orphan data file; it is not loss.

## Update the repository

From INSIDE your existing checked-out repository, use the absolute extracted path:

```bash
git switch -c sprint-03
cp -R "/absolute/path/to/earthquake-sprint-03/repo-update/." .
cat docs/GITIGNORE_SPRINT_03.txt >> .gitignore
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-kafka.txt
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m unittest discover -s tests -v
python scripts/check_code.py
git status --short
```

Review changes, commit ONLY project files (never `.venv`, tokens, downloaded CA
files or `.env`), push the branch and merge to the default branch to expose the
new manual workflow. The append command above preserves existing ignore rules.
`requirements.txt`, smoke files and Sprint1–2 workflow stay as they were.

## Aiven setup (reuse your existing FREE service)

1. Create a separate topic named `earthquake_events_dev`. Use one partition if
   offered; two is supported if the Free plan fixes the count. Keep the service's
   allowed replication/ISR settings; do not assume three broker replicas.
2. Use delete cleanup, with effective retention <=72hours; honor fixed Free-plan
   settings. Runtime inspect refuses unbounded retention or compaction.
3. Your existing SASL user needs Produce, Consume and permission to describe this
   topic's config/metadata; `avnadmin` has default service access. A restricted
   custom user may need DescribeConfigs permission as well as Read/Write and group
   access. Do not change the smoke topic.
4. Reuse the SASL bootstrap endpoint (not the certificate-only endpoint), username,
   password and complete PEM CA that passed the compatibility probe. The CA is
   loaded in memory, not written to a runner certificate file.

Current primary Aiven docs list up to5 topics, two partitions per topic and fixed
Free retention settings. Runtime inspection is authoritative for this service.

## Databricks setup

1. Import/paste `databricks/sprint_03_setup.py` into a Python notebook and Run all
   on Serverless. Expect `PASS Sprint 3 development batch ledger setup`.
2. Import/paste `databricks/sprint_03_sync_handoff_ledger.py` into another notebook.
3. Create a NEW Job `earthquake-dev-handoff-ledger`, max concurrent runs 1, no
   schedule. Add two serial Serverless notebook tasks:
   - `capture_ledger`: existing `sprint_02_sync_capture_ledger` notebook.
   - `handoff_ledger`: new Sprint3 sync notebook, depends on capture_ledger SUCCESS.
4. Add Job parameter `volume_root=/Volumes/earthquake_dev/ops/landing`.
5. Store its numeric Job ID in GitHub variable `EQ_HANDOFF_LEDGER_JOB_ID`.

The existing capture-ledger Job can remain for the source-only workflow. The
new workflow invokes the new two-task Job through the tested Jobs launcher. It
temporarily maps this Job ID to the launcher's existing env name within that step.
No need to edit the existing compatibility or capture Job ID variables.

## GitHub configuration

Repository Settings → Secrets and variables → Actions. Reuse the verified token
VALUE; the earlier 401 was followed by successful source capture/ledger results.
The screenshot Token ID is never a bearer token. Required Databricks scopes files
and jobs still apply. The account running the Job needs the existing dev tables.

| Kind | Name | Value |
| --- | --- | --- |
| Variable, existing | DATABRICKS_HOST | Your matching workspace origin |
| Secret, existing | DATABRICKS_TOKEN | The working token value |
| Variable, existing | EQ_VOLUME_ROOT | /Volumes/earthquake_dev/ops/landing |
| Variable, existing | EQ_COLLECTION_GENERATION | v1 |
| Variable, reuse/add | AIVEN_BOOTSTRAP_SERVERS | The SASL hostname:port from the smoke test |
| Secret, reuse | AIVEN_KAFKA_USERNAME | Working service username |
| Secret, reuse | AIVEN_KAFKA_PASSWORD | Working service password |
| Secret, reuse | AIVEN_KAFKA_CA_PEM | Complete PEM CA, including BEGIN/END lines |
| Variable, NEW | EQ_KAFKA_TOPIC | earthquake_events_dev |
| Variable, NEW | EQ_KAFKA_CONSUMER_GROUP | earthquake-dev-landing-v1 |
| Variable, NEW | EQ_STREAM_GENERATION | v1 |
| Variable, NEW | EQ_HANDOFF_LEDGER_JOB_ID | New two-task Job's numeric ID |

Dispatch generation is a MANUAL WORKFLOW INPUT, default v1. Do not change it for
ordinary retries. A new value deliberately republishes the same source items.

## First cloud run and repeat

Actions → Earthquake Kafka handoff (Sprint3) → Run workflow:

1. action inspect, fail_at none. Expect STREAM_VERIFIED and OFFSET_INSPECTION for
   each partition, then `PASS Sprint 3 inspect`. This action is read-only. Record
   retention.ms, retention.bytes, partition count and the initial bounds.
2. action cycle, mode hourly, window_end blank, dispatch_generation v1,
   fail_at none. The workflow resolves one UTC end time before capture/dispatch.
3. Expect source capture PASS, STREAM_VERIFIED, DISPATCH_VERIFIED,
   LANDING_COMMITTED (when records exist), LANDING_DRAIN_COMPLETE,
   `PASS Sprint 3 dispatch`, `PASS Sprint 3 land`, and finally
   `PASS Sprint 3 Databricks handoff ledger job`.
4. Save the resolved window_end; rerun cycle with that EXACT value and v1. Expect
   source reused true; no new broker publication for receipted chunks; landing
   rows_landed_this_run=0 if no other backlog. Counts/IDs of sealed batches stay
   unchanged. Ledgers must stay unique. Do not assert raw counts equal distinct
   earthquakes: hourly/month/significant feeds and deliberate replays overlap.
5. Run cycle/reconcile with a blank end to exercise both monthly source captures.

If the complete workflow is green, both Databricks tasks succeeded. A launch
message alone is not proof that either task completed. A failure after a sealed
landing does not lose the cloud bytes; correct the notebook/job and run recover.

## Required cloud fault drills

All are manual DEVELOPMENT drills. Keep the same frozen hourly window from the
successful cycle. Each drill uses a distinct dispatch_generation so new records
exist to trigger its fault. First run action cycle with the selected failure;
the expected result is RED with INJECTED_FAILURE, not a normal success.

| Generation example | fail_at | Expected failure evidence | Recovery run |
| --- | --- | --- | --- |
| drill-produce1 | after-produce | Kafka ACK happened but chunk receipt absent; no landing commit | Repeat cycle with SAME window+generation, fail_at none; repeated delivery is allowed |
| drill-data1 | after-data | records.jsonl exists, manifest absent; affected range uncommitted | Repeat cycle, same inputs, none; sealed batches complete without skipped offsets |
| drill-manifest1 | after-manifest | Verified manifest exists, affected range uncommitted | Run recover/none, then land/none; recover repairs the pending commit |
| drill-commit1 | after-commit | Manifest durable, broker offset advanced, run deliberately failed | Run recover/none then land/none; recovery still lists that batch independently of offset |

Use inspect before/after to compare per-partition committed_next. Other already
sealed partitions/ranges may have advanced before a failure; only the unverified
range must remain uncommitted. A drill with no new backlog will not hit its fault
point. Don't delete or alter real capture/manifest bytes to simulate failures.

Cloud interruption at every instruction is not exhaustively proven. These drills
exercise the selected durable boundaries; 59 offline tests cover additional lost
ACK/corruption/out-of-order/budget scenarios. No new broker-cloud run was executed
by the assistant for this delivery.

## SQL acceptance

```sql
SELECT source_id, expected_item_count, dispatch_chunk_count, state,
       accounted_item_count
FROM earthquake_dev.ops.captures;

SELECT batch_id, `partition`, offset_first, offset_last, row_count,
       data_stage, bronze_committed_at
FROM earthquake_dev.ops.batches
ORDER BY stream_generation, `partition`, offset_first;

-- Each query below must return zero rows.
SELECT batch_id, COUNT(*) copies
FROM earthquake_dev.ops.batches GROUP BY batch_id HAVING COUNT(*) > 1;

SELECT capture_id, COUNT(*) copies
FROM earthquake_dev.ops.captures GROUP BY capture_id HAVING COUNT(*) > 1;

SELECT batch_id FROM earthquake_dev.ops.batches
WHERE row_count != offset_last - offset_first + 1
   OR data_stage != 'LANDED' OR bronze_committed_at IS NOT NULL;
```

Before Sprint4: dispatched captures PUBLISH_ACKED (undispatched captures CAPTURED),
accounted_item_count=0, batches LANDED, Bronze counts/timestamps NULL. These states
are intentional: no accepted/rejected business accounting has happened yet.

## Failures and recovery scope

- Wrong credentials/certificate/ACL: fail; fix configuration; reuse the same window
  and dispatch generation. Never weaken TLS or automatically recreate resources.
- Databricks outage: no commit for unverified data. Retry. Broker messages may
  expire during a long outage; retained source captures are the replay evidence.
- Lost Kafka commit ACK: read back committed next offset; accept only exact match.
- Kafka offset gap: fail with OFFSET_GAP; do NOT auto-reset earliest/latest or
  physically delete/recreate the topic. Record incident bounds. This sprint detects
  the gap and preserves frozen evidence; automated, verified gap resolution is a
  Sprint7 reconciliation feature, after Sprint4 source accounting exists.
- Deliberate source replay: action dispatch, original mode+window_end, NEW
  dispatch_generation (for example replay20261015). It republishes retained frozen
  captures without fetching live USGS. Then land normally if the consumer's offset
  bounds remain valid. Replay does not by itself resolve an already-invalid group.
- Topic recreation/rollback of offsets: stop and increment stream generation and
  group under a reviewed recovery plan. Binding does not auto-detect every recreate.
- Ledger sync failure: cloud files are authoritative recovery evidence; rerun
  recover after fixing Spark/Job setup. Insert-only batch MERGE cannot regress a
  future Bronze/Silver stage. No cross-table transaction is assumed.
- Oversized source/message/backlog: fail visibly and retain evidence; do not mark
  the capture ACCOUNTED. Bounds are reviewed configuration/code changes, not skips.

## Documentation checked before implementation

Primary sources, reviewed15October2026:

- Pinned producer API: https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaProducer.html
  Idempotence requires max_in_flight=1 in THIS release; session guarantees do not
  deduplicate a new application's resends. Do not copy current-master defaults.
- Pinned consumer API: https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaConsumer.html
  Explicit assignment/seek, synchronous manual commit and next-offset semantics.
- Pinned admin API: https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaAdminClient.html
  DescribeConfigs returns protocol resources/config entries; topic config ACL needed.
- Installed wheel checked: OffsetAndMetadata(offset,metadata,leader_epoch);
  beginning_offsets/end_offsets accept partitions only; Admin rejects
  allow_auto_create_topics; Producer2.3.2 rejects buffer_memory. Actual-signature
  mocks and supported-config checks guard these exact integration points.
- Aiven Free: https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier
- Aiven Free parameters: https://aiven.io/docs/products/kafka/reference/advanced-params-free-tier
- Aiven Python SASL: https://aiven.io/docs/products/kafka/howto/connect-with-python
- Files API: https://docs.databricks.com/api/files/v2/list-directory-contents
- Delta MERGE: https://docs.databricks.com/aws/en/delta/merge
- GitHub concurrency: https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency

## Completion evidence

59 offline tests passed: the existing30 plus29 Sprint3 cases, including pinned
client APIs, publish receipt replay/corruption, all four injected boundaries,
ambiguous commits, raw binary/null/header preservation, two partitions, fixed end
snapshot, byte splits, budgets, duplicate identities, retention/gap checks.
Syntax/docstring checks passed for21 Python files in the combined repository.
Databricks notebooks are syntax-checked, NOT executed against the user's Spark.
Mark Sprint3 live-validated only after inspect, cycle/retry/reconcile, the four
cloud fault drills and SQL gates all pass. A measured long-running quota/SLA claim
remains outside this gate and is planned for Sprint9.
