# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.results(
# MAGIC         resultId long,
# MAGIC         raceId long,
# MAGIC         driverId long,
# MAGIC         constructorId long,
# MAGIC         number long,
# MAGIC         grid long,
# MAGIC         position long,
# MAGIC         positionText string,
# MAGIC         positionOrder long,
# MAGIC         points double,
# MAGIC         laps long,
# MAGIC         time string,
# MAGIC         milliseconds long,
# MAGIC         fastestLap long,
# MAGIC         rank long,
# MAGIC         fastestLapTime string,
# MAGIC         fastestLapSpeed double,
# MAGIC         statusId long,
# MAGIC         input_file_name string,
# MAGIC         load_timestamp timestamp,
# MAGIC         date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_part)
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/results'

# COMMAND ----------

