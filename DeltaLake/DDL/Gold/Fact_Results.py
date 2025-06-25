# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.goldschema.fact_results(
# MAGIC raceId integer,
# MAGIC driverId integer,
# MAGIC circuitId integer,
# MAGIC date date,
# MAGIC total_points double,
# MAGIC total_wins long
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@sinkdatalakestorage.dfs.core.windows.net/fact_results'