# Databricks notebook source
# MAGIC %md #####Here we will load and write Results data present in datalake storage in Parquet format to Bronze Layer in results folder

# COMMAND ----------

# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

sourcepath='/mnt/source_datalake_storage_gen2/results'
latest_path=get_latest_type2(sourcepath)
datepart=latest_path.split('/')[-1].split('.')[0][-10:].replace('_','-')
df=spark.read.format('parquet').load('/mnt/source_datalake_storage_gen2/results/')
df=f_add_input_filename_loadtime(df,datepart)
df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/results')
# df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/results')

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/results',["resultId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/results')
