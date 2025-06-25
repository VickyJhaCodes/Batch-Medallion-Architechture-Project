# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions

# COMMAND ----------

import requests
from pyspark.sql.functions import current_timestamp,to_date,lit
from datetime import date
from delta.tables import *

# COMMAND ----------

headers = {
    "x-rapidapi-host": f_get_secret("api-host"),
    "x-rapidapi-key": f_get_secret("api-key"),
}
url = "https://v1.formula-1.api-sports.io/teams"
response = requests.get(url, headers=headers).json()
response_data = response["response"]

value_list = []
for r in response_data:
    value_list.append(tuple(list(r.values())))

transformed_list = []
for tup in value_list:
    new_tup = []
    for item in tup:
        if isinstance(item, dict):
            new_tup.extend(item.values())  # Replace dictionary with its values
        else:
            new_tup.append(item)  # Keep non-dictionary items as is
    transformed_list.append(tuple(new_tup))  # Create a new tuple with transformed items


schema = [
    "id",
    "name",
    "logo",
    "base",
    "first_team_entry",
    "world_championships",
    "position",
    "number",
    "pole_positions",
    "fastest_laps",
    "president",
    "director",
    "technical_manager",
    "chassis",
    "engine",
    "tyres",
]

df = spark.createDataFrame(transformed_list, schema)


# df=f_add_input_filename_loadtime(df,"https://v1.formula-1.api-sports.io/teams")

df = f_add_input_filename_loadtime(df, current_timestamp(), "teams")

datepart = date.today()
df.write.partitionBy("date_part").option(
    "replaceWhere", f"date_part = '{datepart}'"
).mode("overwrite").save("/mnt/sink_bronze_datalake_storage_gen2/teams/")

# df.write.mode("overwrite").save('/mnt/sink_bronze_datalake_storage_gen2/teams/')

# COMMAND ----------

f_optimize_zorder('/mnt/sink_bronze_datalake_storage_gen2/teams/',["id"])

# COMMAND ----------

f_vacuum_table('/mnt/sink_bronze_datalake_storage_gen2/teams/')

# COMMAND ----------

