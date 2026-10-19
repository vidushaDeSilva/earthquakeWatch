# Earthquake Watch v1 — phases 0 and 1

This is a clean implementation of the 17 October 2026 specification. It does not import the old controller or run cloud setup on a schedule.

Phase 0: versioned contracts, offline schema validation, non-destructive bootstrap SQL, and a read-only cloud-resource checklist. Phase 1: fetch and validate one USGS `all_day.geojson` response, freeze its exact bytes in a Unity Catalog Volume, verify the read-back, then write the immutable manifest last. No Kafka offsets, Delta tables, or Databricks jobs are modified by the capture command.

```bash
python -m pip install -e '.[test]'
python -m pytest -q
earthquake-watch validate-config
```

For a live capture, first complete `docs/PHASE_0_1_GATE.md`, provision the resources separately, and export `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Then run `earthquake-watch capture --scheduled-time 2026-10-19T19:17:00Z`. The capture prints its identity and path, not the response body or credentials. A repeated capture of the same exact response within the hour is a safe no-op; a changed response has a different ID.

The new code is intentionally limited to phases 0–1. Never run the former `automation_reset.py` or the controller workflow against these resources.
