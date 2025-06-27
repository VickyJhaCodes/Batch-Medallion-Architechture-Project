# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

# f_merge(
#     source_path="/mnt/sink_bronze_datalake_storage_gen2/circuits/",
#     source_table="circuits",
#     target_catalog="layerscatalog",
#     target_schema="silverschema",
#     target_table="circuits",
#     merge_condition="src.circuitId = tgt.circuitId",
#     update_condition="tgt.circuitId = src.circuitId"
# )

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/circuits/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="circuits"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.circuits ZORDER BY circuitId

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.circuits RETAIN 0 HOURS
