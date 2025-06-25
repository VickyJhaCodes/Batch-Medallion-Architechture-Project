# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.race
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/race'

# COMMAND ----------

