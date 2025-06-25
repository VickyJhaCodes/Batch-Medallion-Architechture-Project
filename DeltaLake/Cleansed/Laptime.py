# Databricks notebook source
# bronze mount point: /mnt/sink_bronze_datalake_storage_gen2
# silver mount point: /mnt/sink_silver_datalake_storage_gen2

# COMMAND ----------

# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

# path = get_latest_type3("/mnt/sink_bronze_datalake_storage_gen2/laptimes/")
# df_staging = spark.read.format("delta").load(path)
# df_staging.createOrReplaceTempView("laptimes")

# COMMAND ----------

# %sql
# merge into
#   layerscatalog.silverschema.laptime tgt
# using
#   laptimes src
# on
#   tgt.raceId = src.raceId
#   and tgt.driverId = src.driverId
#   and tgt.lap = src.lap
# when matched then update set tgt.raceId = src.raceId, tgt.driverId = src.driverId, tgt.lap = src.lap
# when not matched then insert *

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/laptimes/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="laptimes"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.laptimes ZORDER BY raceId, driverId, lap

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.laptimes RETAIN 0 HOURS