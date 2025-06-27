# Databricks notebook source
df=spark.read.table("layerscatalog.silverschema.race")
df_final=df.select("raceId","round","circuitId","name","time")
df_final.repartition(1).write.mode("overwrite").save("abfss://gold@sinkdatalakestorage.dfs.core.windows.net/dim_race")
