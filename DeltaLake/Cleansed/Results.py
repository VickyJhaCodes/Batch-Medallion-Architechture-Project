# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

# path = get_latest_type3("/mnt/sink_bronze_datalake_storage_gen2/results/")
# df_staging = spark.read.format("delta").load(path)
# df_staging.createOrReplaceTempView("results")

# COMMAND ----------

# %sql
# merge into
#   layerscatalog.silverschema.results tgt
# using
#   results src
# on
#   tgt.resultId = src.resultId
# when matched then update set tgt.resultId = src.resultId
# when not matched then insert *

# COMMAND ----------

f_merge2(
    source_path="/mnt/sink_bronze_datalake_storage_gen2/results/",
    target_catalog="layerscatalog",
    target_schema="silverschema",
    target_table="results"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE layerscatalog.silverschema.results ZORDER BY resultId

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM layerscatalog.silverschema.results RETAIN 0 HOURS

# COMMAND ----------

