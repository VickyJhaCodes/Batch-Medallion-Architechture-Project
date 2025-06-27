# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.table("layerscatalog.silverschema.drivers")
df_final = df \
    .withColumn("full_name", concat("forename", lit(" "), "surname")) \
    .withColumn("number", (when(col("number") == "\\N", None).otherwise(col("number"))).cast("int")) \
    .withColumn("code", when(col("code") == "\\N", None).otherwise(col("code"))) \
    .drop("url", "input_file_name", "load_timestamp", "date_part", "forename", "surname")

df_final.repartition(1).write.mode("overwrite").save("abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_drivers")
