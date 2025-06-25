# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog layerscatalog

# COMMAND ----------

databases = ["bronzeschema", "silverschema", "goldschema"]
for database in databases:
    tables = (
        spark.sql(f"show tables in layerscatalog.{database}")
        .select("tableName")
        .collect()
    )
    for table in tables:
        if table.tableName != "_sqldf":
            tableName = table.tableName
            spark.sql(f"vacuum layerscatalog.{database}.{tableName}")
            print(f"vacuum command performed for layerscatalog.{database}.{tableName}")

# COMMAND ----------

