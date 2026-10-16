# Databricks notebook source
"""Perform the optional scoped reset and all idempotent setup in one cloud task.

The controller submits this only while ingestion is stopped. The reset journal
is a workspace notebook outside the data catalog and survives this operation.
"""

# COMMAND ----------
# MAGIC %run ./automation_reset

# COMMAND ----------
# MAGIC %run ./sprints_01_02_setup

# COMMAND ----------
# MAGIC %run ./sprint_03_setup

# COMMAND ----------
# MAGIC %run ./sprint_04_setup

# COMMAND ----------
# MAGIC %run ./sprint_07_08_setup

# COMMAND ----------
# MAGIC %run ./sprint_05_06_setup

# COMMAND ----------
for relative in ("runs", "captures", "collections", "handoff", "automation/reports"):
    dbutils.fs.mkdirs("/Volumes/earthquake_dev/ops/landing/" + relative)
print("PASS automated project setup")
