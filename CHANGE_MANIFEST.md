# Gold and publication update

Apply this archive at the root of the existing `earthquake-watch-v1` project.
Inspect local changes first and back up the replaced policy. The archive is an
increment, not a repository snapshot. No existing cloud tables are dropped.

| Action | Repository path | Purpose |
| --- | --- | --- |
| REPLACE | `config/impact_policy_v1.json` | Reviewed thresholds, labels and window |
| NEW | `src/earthquake_watch/gold_policy.py` | Pure deterministic policy and metrics |
| NEW | `databricks/gold_setup.sql` | Add pointer-generation audit table |
| NEW | `databricks/publish_gold.py` | Gold candidate, validation and conditional activation |
| NEW | `tests/test_gold_policy.py` | Pure rule and UTC boundary regressions |

`databricks/phase3_process.py` is unchanged. The Databricks Job gets a new
`publish_gold` Python-script task dependent on its existing Bronze/Silver task.
Both tasks receive the same batch, pipeline run and processing revision from
the Job's caller. The fixed policy path is configured once on the Gold task.

Offline verification: `PYTHONPATH=src python -m pytest -q` passed 24 tests in
the packaging workspace. This does not constitute Databricks, Delta or live
publication acceptance. Do not enable unattended scheduling until the existing
dispatch path and the failure-injection gate have been validated.
