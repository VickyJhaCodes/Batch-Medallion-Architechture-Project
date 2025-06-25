# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/teams/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="teams"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.teams ZORDER BY id

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.teams RETAIN 0 HOURS