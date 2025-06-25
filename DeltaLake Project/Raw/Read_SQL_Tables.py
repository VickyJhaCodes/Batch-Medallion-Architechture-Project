# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

from delta.tables import *
from datetime import date

# COMMAND ----------

jdbcURL=f_get_secret('sourceSQLjdbcURL')
username=f_get_secret('sourceSQLUsername')
password=f_get_secret('sourceSQLpassword')


# COMMAND ----------

qualifying_df=(spark.read \
    .format("jdbc") \
    .option("url",jdbcURL) \
    .option("dbtable",'dbo.qualifying') \
    .option("user",username) \
    .option("password",password) \
    .load()
)
from pyspark.sql.functions import current_timestamp,to_date
from datetime import datetime,date
qualifying_df=f_add_input_filename_loadtime(qualifying_df,current_timestamp(),"dbo.qualifying")
# qualifying_df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/qualifying/')

datepart=date.today()
qualifying_df.write.partitionBy("date_part").option("replaceWhere", f"date_part = '{datepart}'").mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/qualifying')



# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/qualifying/',["qualifyId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/qualifying/')
