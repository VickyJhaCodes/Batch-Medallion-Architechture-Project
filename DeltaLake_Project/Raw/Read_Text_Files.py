# Databricks notebook source
# MAGIC %md #####Here we will load and write Laptimes data present in datalake storage in Text format to Bronze Layer in laptimes folder

# COMMAND ----------

# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions
# MAGIC

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

from pyspark.sql.types import *

schema=StructType([
    StructField("raceId",IntegerType()),
    StructField("driverId",IntegerType()),
    StructField("lap",IntegerType()),
    StructField("position",IntegerType()),
    StructField("time",StringType()),
    StructField("milliseconds",LongType())
])

# COMMAND ----------

# df=spark.read.format('csv').schema(schema).load('/mnt/source_datalake_storage_gen2/laptimes')
sourcepath='/mnt/source_datalake_storage_gen2/laptimes'
latest_path=get_latest_type2(sourcepath)
datepart=latest_path.split('/')[-1].split('.')[0][-10:].replace('_','-')
df=spark.read.format('csv').schema(schema).load(latest_path)
f_validate_schema(df,schema)
df=f_add_input_filename_loadtime(df,datepart)
df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/laptimes')

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/laptimes',['raceId', 'driverId', 'lap'])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/laptimes')
