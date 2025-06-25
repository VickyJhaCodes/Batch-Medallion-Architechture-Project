# Databricks notebook source
# MAGIC %md #####Here we will load and write Race data present in datalake storage in CSV format to Bronze Layer in race folder

# COMMAND ----------

# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions
# MAGIC

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

sourcepath='/mnt/source_datalake_storage_gen2/race'
df=spark.read.format('csv').option('header',True).option('inferSchema',True).load(sourcepath)
latest_path=get_latest_type2(sourcepath)
datepart=latest_path.split('/')[-1].split('.')[0][-10:].replace('_','-')
df=f_add_input_filename_loadtime(df,datepart)
df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/race/')
# df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/race')

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/race/',["raceId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/race/')

# COMMAND ----------

# MAGIC %md #####Here we will load and write Circuits data present in datalake storage in CSV format to Bronze Layer in circuits folder

# COMMAND ----------

sourcepath='/mnt/source_datalake_storage_gen2/circuits/'
df=spark.read.format('csv').option('header',True).option('inferSchema',True).load('/mnt/source_datalake_storage_gen2/circuits')
latest_path=get_latest_type2(sourcepath)
datepart=latest_path.split('/')[-1].split('.')[0][-10:].replace('_','-')
df=f_add_input_filename_loadtime(df,datepart)
df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/circuits/')
# df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/circuits/')


# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/circuits/',["circuitId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/circuits/')

# COMMAND ----------

# MAGIC %md #####Here we will load and write Drivers data present in datalake storage in CSV format to Bronze Layer in drivers folder

# COMMAND ----------

sourcepath='/mnt/source_datalake_storage_gen2/drivers/'
latest_path=get_latest_type2(sourcepath)
datepart=latest_path.split('/')[-1].split('.')[0][-10:].replace('_','-')
df=spark.read.format('csv').option('header',True).option('inferSchema',True).load('/mnt/source_datalake_storage_gen2/drivers')
df=f_add_input_filename_loadtime(df,datepart)
df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/drivers/')
# df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/drivers/')

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/drivers/',["driverId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/drivers/')