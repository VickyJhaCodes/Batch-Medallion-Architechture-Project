# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

try:
    import openpyxl
except ImportError:
    from pip._internal import main as pip
    %pip install openpyxl
    import openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ###### we need to optimize at two levels.
# MAGIC ###### 1. Dynamically take sheet names cannot be taken for me due to path issue in pandas
# MAGIC ###### 2. Also take the data from the latest date folder if it is already loaded skip it else load it.

# COMMAND ----------

# MAGIC %md Here we will see how to take latest folder's data from '/mnt/source_blob_storage/date_part=2023-07-29/constructors.xlsx' as it is a full load of the latest data everyday.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

latest_path = get_latest("/mnt/source_blob_storage/")

sheet_names = ["Sheet1", "Sheet2"]
schema = "constructorId float,constructorRef string,name string,nationality string,url string"
final_df = spark.createDataFrame([], schema=f"{schema}")
excel_path = latest_path
valid_sheet_names = []
for sheet in sheet_names:
    try:
        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .schema(schema)
            .option("dataAddress", f"{sheet}")
            .load(excel_path)
        )
 
        final_df = final_df.union(df)
        valid_sheet_names.append(sheet)
    except Exception as e:
        print("Error Occured:", str(e))
final_df=final_df.distinct()
datepart = latest_path.split("/")[-2].split("=")[1]
final_df = f_add_input_filename_loadtime(final_df, datepart, excel_path.split("/")[-1])
final_df.write.partitionBy("date_part").option(
    "replaceWhere", f"date_part = '{datepart}'"
).mode("overwrite").save("/mnt/sink_bronze_datalake_storage_gen2/constructors/")
# final_df.write.mode('overwrite').option('path','/mnt/sink_bronze_datalake_storage_gen2/constructors/').save()

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/constructors/',["constructorId"])
f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/constructors/')

# COMMAND ----------

# #POC
# from pyspark.sql.functions import current_timestamp

# latest_path = get_latest("/mnt/source_blob_storage/")
# print(latest_path)

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp

# latest_path = get_latest("/mnt/source_blob_storage/")
# print(latest_path)

# COMMAND ----------

# from datetime import datetime


# def get_latest(path: str):
#     try:
#         all_files_list = [
#             (
#                 i.path,
#                 datetime.strptime(i.name.split("=")[1].replace("/", ""), "%Y-%m-%d"),
#             )
#             for i in dbutils.fs.ls(path)
#         ]
#         all_files_list.sort(key=lambda x: x[1], reverse=True)
#         print(all_files_list)
#         return dbutils.fs.ls(all_files_list[0][0].split(":")[1])[0][0].split(":")[1]
#     except Exception as e:
#         print("Error occured: ", e)

# COMMAND ----------

# from datetime import datetime
# allpath=[]
# for i in dbutils.fs.ls("/mnt/source_blob_storage/"):
#     allpath.append((i.path,i.path.split("/")[-2].split("=")[1]))

# allpath.sort(key=lambda x:datetime.strptime(x[1], '%Y-%m-%d'),reverse=True)
# # print(allpath[0][0])
# latest_path=""
# for i in dbutils.fs.ls(allpath[0][0]):
#     latest_path=i.path

# print(latest_path)

