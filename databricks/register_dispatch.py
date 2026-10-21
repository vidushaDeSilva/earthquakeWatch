"""Register a verified dispatch as the RUNNING parent for processing tasks."""

import argparse
from datetime import datetime, timezone
import hashlib
from typing import Any

from earthquake_watch.contracts import ContractError
from earthquake_watch.dispatch import verify_dispatch
from earthquake_watch.volume_client import MountedVolume


TABLE = "earthquake_dev.ops.pipeline_runs"


def register(spark: Any, volume: MountedVolume, batch_id: str, pipeline_run_id: str,
             revision: str, dispatch_id: str, attempt_no: int, databricks_run_id: int) -> None:
    """Merge one stable parent after rechecking intent and landing bytes."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if databricks_run_id <= 0:
        raise ContractError("Invalid Databricks run ID")
    intent = verify_dispatch(volume, dispatch_id, attempt_no, batch_id, pipeline_run_id, revision)
    current = spark.table(TABLE).where(F.col("pipeline_run_id") == pipeline_run_id).collect()
    if len(current) > 1:
        raise ContractError("Duplicate pipeline parent identity")
    if current:
        prior = current[0]
        if (prior.dispatch_id != dispatch_id or prior.dispatch_attempt_no != attempt_no
                or prior.batch_ids != [batch_id] or prior.processing_revision != revision
                or prior.databricks_job_id != intent["job_id"]
                or prior.databricks_run_id != databricks_run_id
                or prior.run_status not in ("RUNNING", "SUCCEEDED", "FAILED")):
            raise ContractError("Existing pipeline parent conflicts with dispatch")
        return
    now = datetime.now(timezone.utc)
    row = dict(pipeline_run_id=pipeline_run_id, dispatch_id=dispatch_id,
               dispatch_attempt_no=attempt_no,
               idempotency_token=hashlib.sha256(
                   f"{dispatch_id}|token|{attempt_no}".encode()).hexdigest(),
               databricks_job_id=intent["job_id"], databricks_run_id=databricks_run_id,
               batch_ids=[batch_id], processing_revision=revision,
               run_status="RUNNING", retryable=None, submitted_at=now,
               started_at=now, finished_at=None, error_class=None,
               error_code=None, error_message=None)
    target = DeltaTable.forName(spark, TABLE)
    source = spark.createDataFrame([row], schema=target.toDF().schema)
    target.alias("t").merge(source.alias("s"), "t.pipeline_run_id = s.pipeline_run_id") \
        .whenNotMatchedInsertAll().execute()
    saved = spark.table(TABLE).where(F.col("pipeline_run_id") == pipeline_run_id).collect()
    if len(saved) != 1 or saved[0].dispatch_id != dispatch_id:
        raise ContractError("Pipeline parent registration could not be confirmed")


def main() -> None:
    """Read the five shared Job parameters passed to the registration task."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--dispatch-id", required=True)
    parser.add_argument("--attempt-no", required=True, type=int)
    parser.add_argument("--databricks-run-id", required=True, type=int)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    register(SparkSession.builder.getOrCreate(), MountedVolume(), args.batch_id,
             args.pipeline_run_id, args.revision, args.dispatch_id, args.attempt_no,
             args.databricks_run_id)
    print("Dispatch parent registered")


if __name__ == "__main__":
    main()
