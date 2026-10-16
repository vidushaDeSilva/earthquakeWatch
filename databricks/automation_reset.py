# Databricks notebook source
"""Erase only the named earthquake development data when explicitly requested.

Never run against other catalogs. Managed-table/Volume deletion follows the
platform's storage lifecycle; it is not an immediate physical-erasure promise.
The external workspace automation journal and source notebooks are preserved.
"""

import re

dbutils.widgets.text("reset_id", "")
dbutils.widgets.text("reset_confirmation", "")
dbutils.widgets.text("reset_smoke", "false")
reset_id = dbutils.widgets.get("reset_id")
if reset_id:
    if (not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", reset_id)
            or dbutils.widgets.get("reset_confirmation") != "RESET_EARTHQUAKE_DEV"):
        raise ValueError("Reset requires a bounded ID and RESET_EARTHQUAKE_DEV")
    tables, volumes, replay_schemas = [], [], []
    catalogs = {r.catalog for r in spark.sql("SHOW CATALOGS").collect()}
    for catalog in ("earthquake_dev", "earthquake_compat"):
        if catalog not in catalogs or (catalog.endswith("compat") and dbutils.widgets.get("reset_smoke") != "true"):
            continue
        schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
        allowed = {"ref", "ops", "bronze", "silver", "gold", "default", "information_schema"}
        for schema in schemas:
            if schema == "information_schema":
                continue
            if schema not in allowed and not re.fullmatch(r"replay_[a-z0-9_]{1,24}", schema):
                raise ValueError("Unexpected schema in project catalog; inspect before reset")
            for item in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
                name = item.tableName
                if item.isTemporary or not re.fullmatch(r"[A-Za-z0-9_]+", name):
                    raise ValueError("Unexpected table identity")
                if catalog == "earthquake_compat" and (schema, name) != ("default", "earthquake_compat_probe"):
                    raise ValueError("Compatibility catalog contains an unknown object")
                if catalog == "earthquake_dev" and schema == "default":
                    raise ValueError("Default schema contains non-pipeline data")
                kind = spark.catalog.getTable(f"{catalog}.{schema}.{name}").tableType
                if kind not in ("VIEW", "MANAGED"):
                    raise ValueError("Reset refuses external data")
                tables.append((catalog, schema, name, kind))
            for item in spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect():
                fields = item.asDict()
                name = fields.get("volume_name") or fields.get("volumeName")
                expected = (catalog, schema, name)
                if expected not in (("earthquake_dev", "ops", "landing"),
                                    ("earthquake_compat", "default", "smoke_test")):
                    raise ValueError("Reset refuses an unknown Volume")
                metadata = spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{name}").collect()
                if len(metadata) != 1 or metadata[0].asDict().get("volumeType") != "MANAGED":
                    raise ValueError("Reset refuses an external Volume")
                volumes.append(expected)
            if schema.startswith("replay_"):
                replay_schemas.append((catalog, schema))
    # Validate the complete inventory before the first destructive statement.
    for catalog, schema, name, kind in sorted(tables, key=lambda v: v[3] != "VIEW"):
        command = "VIEW" if kind == "VIEW" else "TABLE"
        spark.sql(f"DROP {command} IF EXISTS {catalog}.{schema}.{name}")
    for catalog, schema, name in volumes:
        spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{name}")
    for catalog, schema in replay_schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
    print(f"RESET_LOGICAL_DATA_COMPLETE tables={len(tables)} volumes={len(volumes)} reset_id={reset_id}")
