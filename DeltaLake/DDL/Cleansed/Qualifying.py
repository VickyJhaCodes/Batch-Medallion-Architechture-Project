# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.qualifying(
# MAGIC         qualifyId integer,
# MAGIC         raceId integer,
# MAGIC         driverId integer,
# MAGIC         constructorId integer,
# MAGIC         number integer,
# MAGIC         position integer,
# MAGIC         q1 string,
# MAGIC         q2 string,
# MAGIC         q3 string,
# MAGIC         input_file_name string,
# MAGIC         load_timestamp timestamp,
# MAGIC         date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/qualifying'

# COMMAND ----------

