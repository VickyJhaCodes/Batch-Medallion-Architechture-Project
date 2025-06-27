# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.laptimes
# MAGIC (
# MAGIC         raceId integer,
# MAGIC         driverId integer,
# MAGIC         lap integer,
# MAGIC         position integer,
# MAGIC         time string,
# MAGIC         milliseconds long,
# MAGIC         input_file_name string,
# MAGIC         load_timestamp timestamp,
# MAGIC         date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_part)
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/laptimes'

# COMMAND ----------


