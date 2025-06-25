# Databricks notebook source
from pyspark.sql.functions import *
df = spark.read.table("layerscatalog.silverschema.circuits")
final_df = df.select("circuitId", "circuitRef", "name", "location", "country")
final_df.repartition(1).write.mode("overwrite").save("abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_circuits")
