# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.race(
# MAGIC raceId integer,
# MAGIC year integer,
# MAGIC round integer,
# MAGIC circuitId integer,
# MAGIC name string,
# MAGIC date date,
# MAGIC time string,
# MAGIC url string,
# MAGIC fp1_date string,
# MAGIC fp1_time string,
# MAGIC fp2_date string,
# MAGIC fp2_time string,
# MAGIC fp3_date string,
# MAGIC fp3_time string,
# MAGIC quali_date string,
# MAGIC quali_time string,
# MAGIC sprint_date string,
# MAGIC sprint_time string,
# MAGIC input_file_name string,
# MAGIC load_timestamp timestamp,
# MAGIC date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/race'

# COMMAND ----------


