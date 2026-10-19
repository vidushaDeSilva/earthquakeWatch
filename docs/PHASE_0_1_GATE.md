# Phase 0/1 gate (17 October 2026 specification)

## Local evidence

- `earthquake-watch validate-config` validates all versioned configuration files.
- `python -m pytest -q` checks identity, exact byte preservation, metadata rejection, manifest-last ordering, replay, upload interruption, and immutable conflicts.
- `databricks/bootstrap.sql` is reviewed one-time DDL. It never runs from a scheduled workflow. It creates the exact Ops/Bronze/Silver/Gold names from section 12 of the approved specification; constraints are logical, not claimed to be enforced by Delta.

## Live Phase 0 deployment gate (not satisfied by offline tests)

1. Disable old prototype writers; verify intended development resources before applying any bootstrap SQL. Do not reset or delete them automatically.
2. In Databricks, review/apply `bootstrap.sql` once. Verify catalog `earthquake_dev`, schemas `ops`, `bronze`, `silver`, `gold`, all named Delta tables, `silver.events_current`, and managed Volume `earthquake_dev.ops.pipeline_files` with read-only `SHOW`/`DESCRIBE` queries.
3. Grant GitHub identity only the Volume Files API permissions needed for immutable capture, and grant the future Job identity the minimum Delta/Volume privileges. Do not put credentials in repository files.
4. Create Aiven Kafka topic `earthquake_events_dev` with two partitions and 72-hour retention, then verify it read-only. It is provisioned in Phase 0 but not used by Phase 1.
5. Create the preconfigured Databricks Job `earthquake-dev-pipeline` with concurrency one; keep it disabled/not launched until later phases. Verify its job ID, concurrency and identity read-only.
6. Set GitHub variables and secrets named in specification section 10.4, with `EQ_AUTOMATION_ENABLED=false`. Do not schedule or enable the routine workflow yet.

## Live Phase 1 gate

1. Run one explicit capture and inspect the stored `response.json` and `manifest.json` via Files API. Check byte count, SHA256, and `state=FROZEN`.
2. Retry using the same bytes and scheduled hour: the capture ID is unchanged and the first file is never overwritten. If USGS changes the feed during the hour, the new checksum correctly creates another capture.
3. Inject an interruption after response upload, before manifest upload. The same response can be resumed; absent manifest means no committed capture.
4. Confirm no Kafka publish/offset commit, Delta mutation, Job launch, or Gold publication happened in Phase 1.

Only after both gates have evidence should Phase 2 (Kafka transport) begin. The code package does **not** claim that cloud provisioning or live acceptance has been completed.
