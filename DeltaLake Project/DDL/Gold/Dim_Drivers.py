# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.goldschema.dim_drivers(
# MAGIC driverId integer,
# MAGIC driverRef string,
# MAGIC number integer,
# MAGIC code string,
# MAGIC dob date,
# MAGIC nationality string,
# MAGIC full_name string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_drivers'
