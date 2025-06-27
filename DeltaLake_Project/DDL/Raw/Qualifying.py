# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.qualifying
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/qualifying'

# COMMAND ----------


