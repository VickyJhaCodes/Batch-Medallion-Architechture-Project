# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

dbutils.widgets.text("TableName", "")
tableName = dbutils.widgets.get("TableName")

# COMMAND ----------

f_merge2(
    source_path=f"/mnt/sink_bronze_datalake_storage_gen2/{tableName}/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table=f"{tableName}"
)