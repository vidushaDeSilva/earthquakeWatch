# Earthquake Watch dashboard

Read-only Streamlit dashboard for the Phase 5–6 Earthquake Watch repo. Copy the
`dashboard/` and `databricks/dashboard_views.sql` paths from this update into the
root of your current `earthquake-watch-v1` repository. The pipeline code and
Jobs are not changed.

## Deploy the data contract

In the Databricks SQL editor, confirm that `earthquake_dev` contains the Phase
5–6 tables and that `databricks/consumer_views.sql` has already created
`earthquake_dev.gold.active_watchlist` and `active_daily_metrics`. Execute all
six `CREATE OR REPLACE VIEW` statements in `databricks/dashboard_views.sql`.
Each statement is additive to the catalog; no Delta data is changed.

Check that the views exist with fully qualified queries:

```sql
SELECT count(*) FROM earthquake_dev.ops.dashboard_run_summary;
SELECT count(*) FROM earthquake_dev.ops.dashboard_current_publication;
SELECT count(*) FROM earthquake_dev.gold.active_daily_metrics;
```

The `dashboard_current_publication` query should yield at most one row. If it
returns two, investigate duplicate serving pointers before trusting the app.

For eventual public hosting, create a dedicated dashboard identity with only
`USE CATALOG`, `USE SCHEMA`, `CAN USE` on the SQL warehouse, and `SELECT` on
the six dashboard views plus the two active Gold views. Its credentials should
not be the pipeline's write token. Confirm the view owner can read their base
tables. The example grant pattern at the end of `dashboard_views.sql` is a
starting point, not an automatically executed grant.

## Run locally

From the repository root:

```bash
python -m pip install -r dashboard/requirements.txt
cd dashboard
streamlit run app.py
```

Set the following environment variables, or copy
`dashboard/.streamlit/secrets.toml.example` to
`dashboard/.streamlit/secrets.toml` and replace the placeholders:

- `DATABRICKS_HOST`: HTTPS workspace root URL.
- `DATABRICKS_HTTP_PATH`: SQL warehouse HTTP path from Connection details.
- `DATABRICKS_TOKEN`: read-only dashboard identity token.

The dashboard's `.gitignore` excludes its local `secrets.toml`. Do not put
secrets in the example file or in GitHub Actions logs.

## Interpretation and acceptance

- Overview runs and captures cover 30 days and at most the latest 500 each;
  they are not lifetime totals. Stages and checks for the selected run are
  bounded separately. If the window fills, extend the contract before using
  its counts as an exhaustive period readout.
- A GitHub `SUBMITTED` result is only a dispatch. Pipeline success comes from
  terminal `ops.pipeline_runs.run_status` after finalization.
- Kafka acknowledged and landed records are deliveries. They are not unique
  earthquakes; Gold counts are deduplicated by the existing pipeline rules.
- The results page reads only the serving-pointer-gated active Gold views.
  Zero watchlist rows means no event passed the selection policy.
- The dashboard has no GitHub event source, so it cannot certify that seven
  consecutive runs were triggered by `schedule`. Keep the GitHub acceptance
  log separately until the trigger is captured in a governed table.
- Maintenance plans currently live as Volume files rather than a queryable
  ledger. The page states this limitation; it does not fabricate an outcome.

Reconcile the dashboard against the two known successful runs: the second run
`4cd551fcaeafab11aaeced35b314febfdceedd6fc6a660bfd05c719c44c31399`
should show `SUCCEEDED` and publication
`197aede2b78acb855dcad1d60dc417eff99788364c2d4241fcd0de640c118aad`
as active with pointer generation 2, unless a newer successful publication
has since replaced it. Gold daily metrics should reconcile to that currently
active publication, not to all historical Gold rows.

## Host on Streamlit later

Deploy from the same repository with `dashboard/app.py` as the entrypoint and
`dashboard/requirements.txt` as its dependency list. Set the three connection
values in the host's secrets settings. Verify that the dashboard identity can
query the SQL warehouse, that a cold app start stays within your workspace's
compute limits, and that public access does not reveal run errors, raw events,
Volume paths, or credentials. The UI caches bounded query results for up to
five minutes; its displayed refresh time indicates when the app loaded those
results, not when an upstream source generated the earthquakes.
