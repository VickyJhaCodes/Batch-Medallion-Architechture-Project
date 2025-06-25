# Databricks notebook source
df_circuits = spark.read.table('layerscatalog.silverschema.circuits')
df_constructors = spark.read.table('layerscatalog.silverschema.constructors')
df_drivers = spark.read.table('layerscatalog.silverschema.drivers')
df_laptimes = spark.read.table('layerscatalog.silverschema.laptimes')
df_qualifying = spark.read.table('layerscatalog.silverschema.qualifying')
df_race = spark.read.table('layerscatalog.silverschema.race')
df_results = spark.read.table('layerscatalog.silverschema.results')
df_teams = spark.read.table('layerscatalog.silverschema.teams')

# COMMAND ----------

from pyspark.sql.functions import *

df_final = (
    df_results.alias("re")
    .join(broadcast(df_race.alias("ra")), col("ra.raceId") == col("re.raceId"), "inner")
    .join(df_drivers.alias("dr"), col("re.driverId") == col("dr.driverId"), "inner")
).select(
    "re.raceId", "dr.driverId", "ra.circuitId", "ra.date", "re.points", "re.position"
)

df_final_agg = df_final.groupBy("raceId", "driverId", "circuitId", "date").agg(
    sum(col("points")).alias("total_points"),
    sum((when(col("position") == 1, 1).otherwise(0))).alias("total_wins")
)

df_final_agg.write.format("delta").mode("overwrite").save("abfss://gold@sinkdatalakestorage.dfs.core.windows.net/fact_results")