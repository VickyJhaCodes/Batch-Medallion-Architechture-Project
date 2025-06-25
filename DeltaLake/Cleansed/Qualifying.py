# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/qualifying/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="qualifying"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.qualifying ZORDER BY qualifyId

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.qualifying RETAIN 0 HOURS