# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG layerscatalog

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG layerscatalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA silverschema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA layerscatalog.bronzeschema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA layerscatalog.goldschema
