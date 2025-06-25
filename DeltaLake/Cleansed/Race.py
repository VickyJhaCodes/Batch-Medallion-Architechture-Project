# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

# f_merge(
#     source_path="/mnt/sink_bronze_datalake_storage_gen2/race/",
#     source_table="race",
#     target_catalog="layerscatalog",
#     target_schema="silverschema",
#     target_table="race",
#     merge_condition="src.raceId = tgt.raceId",
#     update_condition="tgt.raceId = src.raceId"
# )

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/race/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="race",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.race ZORDER BY raceId

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.race RETAIN 0 HOURS