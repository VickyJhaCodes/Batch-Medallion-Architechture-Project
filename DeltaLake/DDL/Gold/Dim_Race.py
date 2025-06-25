# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.goldschema.dim_race(
# MAGIC raceId integer,
# MAGIC round integer,
# MAGIC circuitId integer,
# MAGIC name string,
# MAGIC time string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_race'