# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

# f_merge(
#     source_path="/mnt/sink_bronze_datalake_storage_gen2/drivers/",
#     source_table="drivers",
#     target_catalog="layerscatalog",
#     target_schema="silverschema",
#     target_table="drivers",
#     merge_condition="src.driverId = tgt.driverId",
#     update_condition="tgt.driverId = src.driverId"
# )

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/drivers/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="drivers"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.drivers ZORDER BY driverId

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.drivers RETAIN 0 HOURS