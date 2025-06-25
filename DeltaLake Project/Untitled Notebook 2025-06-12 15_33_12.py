# Databricks notebook source
#Hello

# COMMAND ----------

import mack

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog layerscatalog

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC use silverschema

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

table_list = [row['tableName'] for row in list_tables.collect()]

# COMMAND ----------

print(table_list)

# COMMAND ----------

import mack as mk

# COMMAND ----------


for table in table_list:
    if table != '_sqldf':
        key=mk.find_composite_key_candidates(spark.table(table))
        print(f"Table: {table} has composite key: {key}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from layerscatalog.bronzeschema.constructors order by constructorId

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from layerscatalog.bronzeschema.constructors order by constructorId

# COMMAND ----------

#Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from layerscatalog.silverschema.results

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   driverId,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when position = 1 then 1
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as num_wins,
# MAGIC   sum(points) as total_points
# MAGIC from
# MAGIC   layerscatalog.silverschema.results
# MAGIC group by
# MAGIC   driverId
# MAGIC order by
# MAGIC   num_wins desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from layerscatalog.silverschema.race
