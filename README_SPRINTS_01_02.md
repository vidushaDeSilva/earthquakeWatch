# Earthquake project — Sprints 1–2

This additive update implements documented source contracts and resilient, immutable USGS capture into the development Databricks Volume. It keeps the original compatibility smoke test available.

Start with [the installation and acceptance steps](docs/SPRINT_01_02_HANDOFF.md). The [project memory](docs/PROJECT_MEMORY.md) is the continuing design baseline, and [coding rules](docs/CODING_STANDARD.md) apply to future sprints.

Run offline verification with Python 3.12:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 scripts/check_code.py
```

No extra runtime packages are needed for Sprints 1–2. Use GitHub Actions to fetch actual source data; it remains in memory until its verified cloud capture. This update adds no automatic schedule, Kafka dispatch, business-layer tables or data deletion.
