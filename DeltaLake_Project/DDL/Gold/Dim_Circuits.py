# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.goldschema.dim_circuits(
# MAGIC circuitId integer,
# MAGIC circuitRef string,
# MAGIC name string,
# MAGIC location string,
# MAGIC country string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_circuits'
