"""Read-only Earthquake Watch dashboard for local and hosted Streamlit use."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

# Streamlit runs this script by path; make the repository root importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard import queries  # noqa: E402
from dashboard.content import load_context, watchlist_explanation  # noqa: E402
from dashboard.data import fetch_rows  # noqa: E402
from dashboard.metrics import (as_utc, duration_minutes, landing_coverage,
                               summarize)  # noqa: E402


st.set_page_config(page_title="Earthquake Watch", page_icon="🌍", layout="wide")


@st.cache_data(ttl=300, show_spinner=False)
def load(query: str, parameters: tuple[str, ...] = ()) -> list[dict]:
    """Cache only bounded results for five minutes; never cache credentials."""
    return fetch_rows(query, parameters)


@st.cache_data(ttl=300, show_spinner=False)
def snapshot() -> dict:
    """Load the six top-level datasets as one dashboard cache generation."""
    return {
        "runs": load(queries.RUNS),
        "captures": load(queries.CAPTURES),
        "batches": load(queries.BATCHES),
        "current": load(queries.CURRENT_PUBLICATION),
        "daily": load(queries.DAILY),
        "watchlist": load(queries.WATCHLIST),
        "loaded_at": datetime.now(timezone.utc),
    }


def timestamp(value: object) -> str:
    """Display timestamps consistently in UTC."""
    instant = as_utc(value)
    return instant.strftime("%Y-%m-%d %H:%M UTC") if instant else "Unavailable"


def table(rows: list[dict], columns: list[str]) -> None:
    """Render an exact, bounded record table without changing its grain."""
    if rows:
        st.dataframe(pd.DataFrame(rows).reindex(columns=columns),
                     use_container_width=True, hide_index=True)
    else:
        st.info("No records for this view.")


def overview(runs: list[dict], captures: list[dict], batches: list[dict],
             current: list[dict]) -> None:
    """Answer arrival, processing, and publication status at a glance."""
    summary = summarize(runs, captures, batches)
    capture = summary["latest_capture"] or {}
    latest = summary["latest_run"] or {}
    success = summary["latest_success"] or {}
    active = current[0] if len(current) == 1 else {}

    a, b, c, d = st.columns(4)
    a.metric("Latest capture", timestamp(capture.get("fetched_at")))
    b.metric("Latest run", latest.get("run_status") or "No runs")
    c.metric("Last successful run", timestamp(success.get("finished_at")))
    d.metric("Active pointer", active.get("pointer_generation") if active else "None")

    st.caption("The latest run may still be processing. A successful dispatch alone is not a completed pipeline run.")
    if len(current) > 1:
        st.error("Duplicate serving pointers: publication status requires investigation.")
    elif active and active.get("publication_status") == "ACTIVE" and active.get("validation_status") == "PASS":
        st.success(f"Serving publication {active['publication_id']} · activated {timestamp(active.get('activated_at'))}")
    else:
        st.warning("No validated active publication is available.")

    st.subheader("Pipeline outcomes · last 30 days")
    st.write(f"{summary['success_count']} succeeded · "
             f"{summary['terminal_count'] - summary['success_count']} failed · "
             f"{summary['pending_count']} nonterminal (up to 500 most recent runs)")
    if runs:
        outcomes = pd.DataFrame([{
            "Day (UTC)": as_utc(r.get("submitted_at")).date(),
            "Status": r.get("run_status"), "Runs": 1
        } for r in runs if as_utc(r.get("submitted_at"))])
        if not outcomes.empty:
            by_day = outcomes.pivot_table(index="Day (UTC)", columns="Status",
                                          values="Runs", aggfunc="sum", fill_value=0)
            st.bar_chart(by_day)
        history = pd.DataFrame([{
            "Submitted (UTC)": as_utc(r.get("submitted_at")),
            "Status": r.get("run_status"), "Duration (min)": duration_minutes(r),
            "Run ID": r.get("pipeline_run_id")
        } for r in runs])
        st.dataframe(history.head(30), use_container_width=True, hide_index=True)

    st.subheader("Transport evidence")
    batch = summary["latest_batch"]
    if batch:
        st.write(f"Latest landing batch: {batch['landing_batch_id']} · "
                 f"landed / expected deliveries: {landing_coverage(batch)} · "
                 f"status: {batch.get('landing_status')}")
    else:
        st.info("No recent landing batch found.")


def pipeline(runs: list[dict], batches: list[dict], captures: list[dict]) -> None:
    """Provide one-run drill-down without conflating attempts and publications."""
    st.subheader("Pipeline runs · last 30 days")
    statuses = sorted({str(r.get("run_status")) for r in runs})
    selected = st.multiselect("Run status", statuses, default=statuses)
    filtered = [r for r in runs if str(r.get("run_status")) in selected]
    table([{**r, "duration_min": duration_minutes(r)} for r in filtered], [
        "submitted_at", "pipeline_run_id", "run_status", "duration_min",
        "processing_revision", "databricks_run_id", "publication_id",
        "is_active_publication", "error_code"])
    if not filtered:
        return

    labels = {r["pipeline_run_id"]: f"{timestamp(r.get('submitted_at'))} · "
              f"{r.get('run_status')} · {r['pipeline_run_id'][:12]}" for r in filtered}
    run_id = st.selectbox("Inspect a run", list(labels), format_func=labels.get)
    run = next(r for r in filtered if r["pipeline_run_id"] == run_id)
    st.write(f"**Run ID:** `{run_id}` · **Databricks run:** {run.get('databricks_run_id')} · "
             f"**Publication:** {run.get('publication_id') or 'None'}")
    try:
        stages, checks = load(queries.STAGES, (run_id,)), load(queries.CHECKS, (run_id,))
    except Exception:
        st.error("Could not load stage or quality evidence. Check the dashboard views and access grants.")
        return
    st.markdown("#### Stages")
    table(stages, ["stage_name", "attempt_number", "stage_status", "records_in",
                   "records_out", "records_quarantined", "output_delta_version",
                   "error_code", "started_at", "finished_at"])
    st.markdown("#### Quality checks")
    table(checks, ["stage_name", "rule_id", "severity", "check_status",
                   "observed_value", "expected_value", "checked_at"])

    ids = set(run.get("batch_ids") or [])
    matching = [b for b in batches if b.get("landing_batch_id") in ids]
    st.markdown("#### Landing evidence")
    table(matching, ["landing_batch_id", "landing_status", "expected_record_count",
                     "landed_record_count", "partition_ranges", "created_at", "verified_at"])
    capture_ids = {c for b in matching for c in b.get("capture_ids") or []}
    st.markdown("#### Capture evidence")
    table([c for c in captures if c.get("capture_id") in capture_ids], [
        "capture_id", "source_id", "schedule_slot_start", "source_generated_at",
        "fetched_at", "expected_event_count", "capture_status"])


def results(daily: list[dict], watchlist: list[dict]) -> None:
    """Display only pointer-gated, validated Gold results."""
    st.subheader("Published earthquake results")
    try:
        _, policy = load_context()
        scope, rules = watchlist_explanation(policy)
        st.markdown(f"**What counts as a watchlist earthquake?** {scope}")
        st.markdown("\n".join(f"- {rule}" for rule in rules))
        st.caption("PAGER red, orange, or yellow gives the label 'estimated impact'; "
                   "the other signals alone give 'potential impact'. These are "
                   "prioritization labels, not a confirmation of damage or an alert.")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        st.error(f"Could not load the impact policy explanation: {type(exc).__name__}. "
                 "Check config/impact_policy_v1.json in this repository.")
    if daily:
        publication_ids = {r["publication_id"] for r in daily}
        st.caption(f"Active publication: {', '.join(sorted(publication_ids))} · "
                   f"as of {timestamp(max((r.get('as_of_utc') for r in daily if r.get('as_of_utc')), default=None))}")
        dates = sorted(r["metric_date"] for r in daily)
        selected_dates = st.date_input("Metric dates (UTC)", value=(dates[0], dates[-1]),
                                       min_value=dates[0], max_value=dates[-1])
        start, end = (selected_dates if len(selected_dates) == 2
                      else (dates[0], dates[-1]))
        shown_daily = [r for r in daily if start <= r["metric_date"] <= end]
        frame = pd.DataFrame(shown_daily).sort_values("metric_date")
        if not shown_daily:
            st.info("No daily metrics for those dates.")
        else:
            st.bar_chart(frame.set_index("metric_date")[["event_count", "watchlist_count"]])
        table(shown_daily, ["metric_date", "event_count", "watchlist_count", "max_magnitude",
                            "pager_yellow_count", "pager_orange_count", "pager_red_count",
                            "tsunami_flag_count", "coverage_status", "as_of_utc"])
    else:
        st.info("There are no rows in the active daily metrics view.")

    st.subheader("Current watchlist")
    if not watchlist:
        st.info("No earthquakes met the watchlist policy in the active publication.")
        return
    labels = sorted({r.get("impact_label") for r in watchlist if r.get("impact_label")})
    selected = st.multiselect("Impact label", labels, default=labels)
    shown = [r for r in watchlist if r.get("impact_label") in selected]
    table(shown, ["event_time", "place", "magnitude", "significance", "pager_alert",
                  "impact_label", "selection_reasons", "rank_priority", "depth_km",
                  "latitude", "longitude", "usgs_event_id"])
    if shown:
        st.map(pd.DataFrame(shown)[["latitude", "longitude"]].dropna(),
               latitude="latitude", longitude="longitude")


def main() -> None:
    """Load one bounded snapshot and expose three read-only dashboard pages."""
    st.title("Earthquake Watch")
    try:
        source, _ = load_context()
        st.markdown("Earthquake Watch polls the [USGS all-day GeoJSON feed]"
                    f"({source['source_url']}) each hour. Each source record describes "
                    "an earthquake's origin time, location, magnitude, depth and "
                    "available impact signals. The pipeline transports deliveries "
                    "through Kafka, resolves current event versions in Databricks, "
                    "and publishes a validated rolling 24-hour summary.")
    except (OSError, ValueError, KeyError, TypeError):
        st.warning("Source description unavailable. Check config/source_usgs_all_day.json.")
    if st.sidebar.button("Refresh data"):
        load.clear()
        snapshot.clear()
        st.rerun()
    page = st.sidebar.radio("View", ["Overview", "Pipeline runs", "Earthquake results"])
    try:
        data = snapshot()
    except Exception as exc:
        # Show only a sanitized diagnostic. Connector errors can contain SQL
        # text, but must never expose token values or connection credentials.
        message = str(exc)
        for secret_name in ("DATABRICKS_TOKEN", "AIVEN_KAFKA_PASSWORD", "AIVEN_KAFKA_CA_PEM"):
            try:
                from dashboard.data import _setting
                secret = _setting(secret_name)
                if secret:
                    message = message.replace(secret, "[redacted]")
            except Exception:
                pass
        st.error("Dashboard data is unavailable.")
        st.code(f"{type(exc).__name__}: {message}")
        st.markdown("""
**Check these items:**

1. Start the app from `dashboard/` so `.streamlit/secrets.toml` is loaded:
   `cd dashboard && streamlit run app.py`.
2. Confirm the SQL warehouse is running and that `DATABRICKS_HTTP_PATH` starts
   with `/sql/`.
3. Execute `databricks/dashboard_views.sql` in Databricks SQL Editor.
4. Confirm `earthquake_dev.gold.active_daily_metrics` and
   `earthquake_dev.gold.active_watchlist` exist.
""")
        st.stop()
    st.caption(f"Dashboard data loaded: {data['loaded_at']:%Y-%m-%d %H:%M UTC} "
               "· query cache up to 5 minutes · pipeline runs limited to the latest 500 in 30 days")
    if page == "Overview":
        overview(data["runs"], data["captures"], data["batches"], data["current"])
    elif page == "Pipeline runs":
        pipeline(data["runs"], data["batches"], data["captures"])
    elif page == "Earthquake results":
        results(data["daily"], data["watchlist"])


if __name__ == "__main__":
    main()
