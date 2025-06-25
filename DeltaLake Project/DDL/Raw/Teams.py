# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS layerscatalog.bronzeschema.teams(
# MAGIC         id long,
# MAGIC         name string,
# MAGIC         logo string,
# MAGIC         base string,
# MAGIC         first_team_entry long,
# MAGIC         world_championships long,
# MAGIC         position long,
# MAGIC         number long,
# MAGIC         pole_positions long,
# MAGIC         fastest_laps long,
# MAGIC         president string,
# MAGIC         director string,
# MAGIC         technical_manager string,
# MAGIC         chassis string,
# MAGIC         engine string,
# MAGIC         tyres string,
# MAGIC         input_file_name string,
# MAGIC         load_timestamp timestamp,
# MAGIC         date_part date     
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_part)
# MAGIC LOCATION 'abfss://bronze@sinkdatalakestorage.dfs.core.windows.net/teams'

# COMMAND ----------


