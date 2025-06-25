# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

list_silver_tables = [
    "circuits",
    "constructors",
    "drivers",
    "laptimes",
    "qualifying",
    "race",
    "results",
    "teams",
]

for tableName in list_silver_tables:
    f_merge2(
        source_path=f"/mnt/sink_bronze_datalake_storage_gen2/{tableName}/",
        target_catalog="layerscatalog",
        target_schema="silverschema",
        target_table=f"{tableName}",
    )
    print(f"{tableName} silver table is merged.")

# COMMAND ----------

