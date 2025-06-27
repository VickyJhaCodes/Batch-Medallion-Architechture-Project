# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS quickstart_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG quickstart_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA quickstart_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED quickstart_schema;

# COMMAND ----------


