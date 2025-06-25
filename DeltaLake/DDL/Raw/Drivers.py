# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.drivers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/drivers'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended layerscatalog.bronzeschema.drivers;

# COMMAND ----------

