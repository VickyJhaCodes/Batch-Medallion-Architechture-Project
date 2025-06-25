# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.drivers(
# MAGIC driverId integer,
# MAGIC driverRef string,
# MAGIC number string,
# MAGIC code string,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC dob date,
# MAGIC nationality string,
# MAGIC url string,
# MAGIC input_file_name string,
# MAGIC load_timestamp timestamp,
# MAGIC date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/drivers'

# COMMAND ----------


