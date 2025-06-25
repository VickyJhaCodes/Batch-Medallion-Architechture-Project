# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.constructors(
# MAGIC constructorId float,
# MAGIC constructorRef string,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC url string,
# MAGIC input_file_name string,
# MAGIC load_timestamp timestamp,
# MAGIC date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/constructors'

# COMMAND ----------

