# Earthquake Watch

Automated USGS → Aiven Kafka → Databricks Volume → Bronze/Silver/Gold pipeline.
There is one operational workflow: **Earthquake automated pipeline**.

**This delivery is configured to erase the named earthquake development and
smoke-test data once on its first deployment, as requested.** Review
`config/automation.json` before pushing. It preserves unrelated resources,
credentials, source code and its small automation/reset journal.

## Install once

The complete repository ZIP contains an `earthquake-watch` folder. Copy it into
your existing Git repository; your `.git` directory is not included or replaced.
Review overlapping custom edits before copying.

```bash
cd "/Users/vidushadesilva/Desktop/projects/earthquake watch"
git switch -c automated-pipeline
cp -R "$HOME/Downloads/earthquake-watch/." .
git diff --stat
```

Commit the project files, push and merge into your default branch (`main` or
`master`). The workflow checks code/tests before making cloud changes. There
are no local data collectors or manually executed Databricks setup notebooks.
The changed-files ZIP is an alternative to the complete snapshot, not another
installation step.

## Existing credentials

Keep these GitHub repository variables:

- `DATABRICKS_HOST`
- `AIVEN_BOOTSTRAP_SERVERS`

Keep these GitHub repository secrets:

- `DATABRICKS_TOKEN`
- `AIVEN_KAFKA_USERNAME`
- `AIVEN_KAFKA_PASSWORD`
- `AIVEN_KAFKA_CA_PEM`

No new secret is required. Job IDs are discovered by exact name. Existing
`EQ_HANDOFF_LEDGER_JOB_ID` and `EQ_CAPTURE_LEDGER_JOB_ID` variables are optional
compatibility hints; if present, they must identify the known project Jobs.
Stream generation, consumer group and table/Volume paths are managed by code.

## What happens after the merge

1. The workflow tests the repository and reads its saved Databricks checkpoint.
2. On first deployment, it stops existing project writers, clears the named
   development/smoke assets, and automatically rebuilds the database and Job.
3. Scheduled ticks collect frozen USGS responses, dispatch/land Kafka records,
   launch processing, and check the same Databricks run until it finishes.
4. Remaining batches continue automatically. Transient outages get retry/backoff;
   repeated connection failures open a cooldown that retries automatically.
5. Reconciliation controls publication. Healthy results publish and get snapshots
   and safe bounded retention. Incomplete or conflicting evidence cannot publish.

You do not run capture/recover/cycle/wait scripts or copy run IDs. The native
GitHub summary shows controller status and Databricks provides task/Delta audit.
`WORK_IN_PROGRESS` is a valid controller result; `COMPLETE` means a gated release
finished. Fatal permission, code or data-quality errors are surfaced as incidents.

The schedule checks every30 minutes; a new source cycle starts one hour after
the prior cycle completes. Long Spark work does not keep a GitHub runner waiting.
Source revisions remain history, and older replays cannot overwrite newer state.

## Reset semantics

The initial reset ID is `clean-20261016`. The cloud journal records it as complete,
so ordinary pushes and scheduled ticks never repeat that reset. A later explicit
reset is available in the same workflow, but is not needed for routine operation.
Keep the journal; deleting it can re-arm the configured first-deployment reset.

The reset clears managed tables/views and the landing Volume in `earthquake_dev`,
known compatibility table/Volume data in `earthquake_compat`, the two named
earthquake Kafka topics and matching inactive project groups. It recreates the
main topic with a new logical stream generation. It does not delete the Aiven
service/account, unrelated catalogs/topics, credentials or source notebooks.
Unexpected/external resources fail the reset rather than broadening deletion.
Dropped Volumes and Kafka messages are destructive; managed physical file purge
follows the provider lifecycle. Empty catalog/schema containers and reset audit
metadata intentionally remain.

## Limits and verification

The code targets free services. Databricks quotas and GitHub scheduling delays
remain outside the pipeline's control; standard public-repository Actions
runners avoid private-minute limits. The system never enables paid compute.
No free service provides an uninterrupted-operation guarantee here.

See `docs/AUTOMATION_V2.md` for recovery states, exact reset boundaries, source
documentation and acceptance checks. Older sprint handoffs describe historical
steps; they are not the current operating procedure.

Offline tests exercise orchestration, reset guards and the actual data engines.
Actual deployment, destructive reset, Serverless execution and sustained free-tier
usage must still be validated in your workspace. This package has not deleted
your cloud data from the assistant session.
