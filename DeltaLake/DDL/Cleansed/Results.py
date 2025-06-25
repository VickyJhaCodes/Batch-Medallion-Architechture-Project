# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.results(
# MAGIC resultId int,
# MAGIC raceId int,
# MAGIC driverId int,
# MAGIC constructorId int,
# MAGIC number int,
# MAGIC grid int,
# MAGIC position int,
# MAGIC positionText string,
# MAGIC positionOrder int,
# MAGIC points double,
# MAGIC laps int,
# MAGIC time string,
# MAGIC milliseconds int,
# MAGIC fastestLap int,
# MAGIC rank int,
# MAGIC fastestLapTime string,
# MAGIC fastestLapSpeed double,
# MAGIC statusId int,
# MAGIC input_file_name string,
# MAGIC load_timestamp timestamp,
# MAGIC date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/results'

# COMMAND ----------

