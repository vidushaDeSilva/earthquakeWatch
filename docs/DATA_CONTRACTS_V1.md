# Sprint 1 source and capture contracts

## Scope

Sprints 1–2 implement configuration, deterministic IDs, source fetching, immutable Databricks Volume capture and operations metadata. Kafka publication starts in Sprint 3. Bronze ingestion starts in Sprint 4. Production scheduling remains disabled until those safety gates pass.

Source modes are defined by `quake_pipeline.contracts.SOURCES`:

| Mode | Feeds | Collection interval |
| --- | --- | --- |
| hourly | `2.5_day.geojson` | One aligned UTC hour |
| reconcile | `2.5_month.geojson`, `significant_month.geojson` | One UTC day, ending at midnight |

The collection interval is a retry/scheduling key, not a claim that the live feed contains only earthquakes from that hour/day. Event coverage comes from the feed's moving window. An uncaptured old interval cannot be reconstructed using a current live feed; historical catalog backfill is Sprint 7.

## Identities and immutable boundaries

`collection_id = SHA256(JSON tuple [kind, environment, source, revision, mode, start, end, URL, contract, generation])`.

`capture_id = SHA256(JSON tuple [capture-v1, collection_id, exact response SHA256])`.

In Sprint 3, `source_item_id` will hash capture ID and ordinal. Source-version IDs already have a tested helper: exact source ID + updated milliseconds + a common semantic projection. Summary/detail wrappers, URLs, alias metadata and detail-only product arrays do not change semantic hashes; equivalent numeric representations do not change them either.

All data stays in memory until uploaded to `/Volumes/earthquake_dev/ops/landing`:

| Path | Meaning |
| --- | --- |
| `captures/<collection_id>/<capture_id>/response.json` | Exact source response bytes |
| `captures/<collection_id>/<capture_id>/ready.json` | Verified source manifest, written after response verification |
| `collections/<collection_id>/selected.json` | Immutable chosen capture for this collection |
| `runs/<run_id>/report.json` | One immutable terminal metadata report; includes request attempts, not payloads |

Retry first checks the selected capture and verifies its bytes. If selection was interrupted, list the collection's capture directories, follow all pagination tokens, validate complete ready manifests and choose the earliest complete capture deterministically. Never refetch to replace a broken selected capture. Unsealed partial paths do not count as completed captures.

## Ready manifest v1

Fields: `manifest_version`, `contract_version`, `collection_id`, `capture_id`, `environment`, `source_id`, `source_revision`, `feed_key`, `run_kind`, `first_run_id`, `window_start`, `window_end`, `fetched_at`, `source_generated_at`, `source_url`, `response_sha256`, `response_bytes`, `expected_item_count`, `response_relative_path`, `source_stale`, `state`.

The manifest contains no credentials. It records original fetch time; reusing a capture never advances source freshness. `state=CAPTURED` means source evidence is stored, not that Kafka/Bronze have processed it. Expected item count includes malformed individual items so later quarantine accounting cannot silently lose them.

## HTTP and validation contract

- Standard-library HTTP, default TLS verification, no automatic redirects, no extra runtime dependencies.
- Maximum five attempts/request, 100 request attempts/run, 16 MiB/response, 30-second socket timeout, 20-minute overall request budget.
- Retry transient transport errors, 408, 429 and 5xx. Treat invalid certificates, 401/403 and other unexpected nonretryable statuses as terminal.
- Honor Retry-After delta seconds and HTTP dates. Defer if required waiting exceeds 60 seconds or the remaining run budget; do not shorten the provider's requested delay.
- Source summary must be strict UTF-8 JSON FeatureCollection, with a features array, valid metadata count, generated timestamp and status 200. A count mismatch, blank body, non-JSON body or NaN/Infinity fails visibly. A well-formed zero-item feed succeeds.
- A summary HTTP 204 is not accepted as a successful empty feed. Catalog-query 204 semantics will be added with the catalog backfill in Sprint 7.
- Individual event typing/rejection is deferred to Bronze, while the complete source response is preserved. Future generation times fail; generated data older than two hours is marked stale.
- Databricks writes always use `overwrite=false`, then read back and compare hashes. Existing identical bytes are a successful retry; different bytes are a conflict.

## Capture metadata import

The ledger notebook uses explicit JSON schemas, independently verifies response checksums through Spark binaryFile reads and normalizes Volume path URI prefixes. It inserts Ops rows idempotently, checks target-key uniqueness and never regresses a later capture state. Serverless execution and SQL are live-acceptance gates; local tests do not emulate Spark.
