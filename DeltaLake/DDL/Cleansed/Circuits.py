# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.silverschema.circuits(
# MAGIC circuitId integer,
# MAGIC circuitRef string,
# MAGIC name string,
# MAGIC location string,
# MAGIC country string,
# MAGIC lat double,
# MAGIC lng double,
# MAGIC alt string,
# MAGIC url string,
# MAGIC input_file_name string,
# MAGIC load_timestamp timestamp,
# MAGIC date_part date
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silvercontainer@sinkdatalakestorage.dfs.core.windows.net/circuits'

# COMMAND ----------

