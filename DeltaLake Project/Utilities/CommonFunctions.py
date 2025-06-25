# Databricks notebook source
def f_get_secret(key):
    try:
        return dbutils.secrets.get(scope="sinkSecretScope", key=f"{key}")
    except Exception as e:
        print(f"Error: {str(e)}")

# COMMAND ----------

from datetime import datetime


def get_latest(path: str):
    try:
        all_files_list = [
            (
                i.path,
                datetime.strptime(i.name.split("=")[1].replace("/", ""), "%Y-%m-%d"),
            )
            for i in dbutils.fs.ls(path)
        ]
        all_files_list.sort(key=lambda x: x[1], reverse=True)
        return dbutils.fs.ls(all_files_list[0][0].split(":")[1])[0][0].split(":")[1]
    except Exception as e:
        print("Error occured: ", e)

# COMMAND ----------

# def f_add_input_filename_loadtime(df,filename=None):
#     from pyspark.sql.functions import input_file_name,current_timestamp,split,length,lit
#     if filename is None:
#         df_final=df.withColumn("input_file_name", split(input_file_name(), '/')[4]).withColumn("load_timestamp",current_timestamp())
#         return df_final
#     else:
#         df_final=df.withColumn("input_file_name", lit(filename)).withColumn("load_timestamp",current_timestamp())
#         return df_final

# COMMAND ----------

def f_add_input_filename_loadtime(df, datepart, filename=None):
    from pyspark.sql.functions import (
        input_file_name,
        current_timestamp,
        split,
        length,
        lit,
        to_date,
    )

    if filename is None:
        df_final = (
            df.withColumn("input_file_name", split(input_file_name(), "/")[4])
            .withColumn("load_timestamp", current_timestamp())
            .withColumn("date_part", to_date(lit(datepart), "yyyy-MM-dd"))
        )
        return df_final
    else:
        df_final = (
            df.withColumn("input_file_name", lit(filename))
            .withColumn("load_timestamp", current_timestamp())
            .withColumn("date_part", to_date(lit(datepart), "yyyy-MM-dd"))
        )
        return df_final

# COMMAND ----------

from pyspark.sql.types import *


def f_validate_schema(df, sinkschema):
    sourceschema = df.limit(1).schema
    if sinkschema == sourceschema:
        return 0
    else:
        raise Exception("Schema Mismatch has happened.")

# COMMAND ----------

def get_latest_type2(sourcepath):
    datelist = []
    latest_file_path = ""
    for i in dbutils.fs.ls(f"{sourcepath}"):
        datelist.append(
            (
                datetime.strptime(
                    i.name.split(".")[0][-10:].replace("_", "-"), "%Y-%m-%d"
                ),
                i.path,
            )
        )
    datelist.sort()
    datelist.reverse()
    latest_file_path = datelist[0][1]
    return latest_file_path

# COMMAND ----------

from datetime import datetime


def get_latest_path_stream(sourcepath: str):
    list_path_dates = []
    for i in dbutils.fs.ls(sourcepath):
        list_path_dates.append(
            (
                i.path,
                i.name,
                datetime.strptime(
                    i.name.split(".")[0][-19:-9].replace("_", "-"), "%Y-%m-%d"
                ).date(),
                datetime.strptime(i.name.split(".")[0][-8:], "%H_%M_%S"),
            )
        )

    list_path_dates.sort(key=lambda x: x[2])
    list_path_dates.reverse()

    return list_path_dates[0][0]

# COMMAND ----------

def get_latest_type3(sourcepath):
    datelist = []
    latest_file_path = ""
    for i in dbutils.fs.ls(f"{sourcepath}"):
        if i.name != "_delta_log/":
            datelist.append(
                (
                    datetime.strptime(
                        i.name.split(".")[0][-10:].replace("_", "-"), "%Y-%m-%d"
                    ),
                    i.path,
                )
            )
    datelist.sort()
    datelist.reverse()
    latest_file_path = datelist[0][1]
    return latest_file_path

# COMMAND ----------

def get_latest_type3(sourcepath):
    datelist = []
    latest_file_path = ""
    for i in dbutils.fs.ls(f"{sourcepath}"):
        if i.name != "_delta_log/":
            # datelist.append((datetime.strptime(i.name.split('.')[0][-10:].replace('_','-'),'%Y-%m-%d'),i.path))
            datelist.append(
                (
                    datetime.strptime(i.name.split("/")[-2].split("=")[1], "%Y-%m-%d"),
                    i.path,
                )
            )
    datelist.sort()
    datelist.reverse()
    latest_file_path = datelist[0][1]
    return latest_file_path

# COMMAND ----------

def f_merge(
    source_path,
    source_table,
    target_catalog,
    target_schema,
    target_table,
    merge_condition,
    update_condition,
):
    path = get_latest_type3(f"{source_path}")
    df_staging = spark.read.format("delta").load(path)
    df_staging.createOrReplaceTempView(f"{source_table}")
    merge_query = """
    merge into {0}.{1}.{2} tgt using {3} src on {4}
    when matched then update set {5}
    when not matched then insert *
    """.format(
        target_catalog,
        target_schema,
        target_table,
        source_table,
        merge_condition,
        update_condition,
    )
    spark.sql(merge_query)

# COMMAND ----------

from delta.tables import DeltaTable


def f_merge2(
    source_path,
    target_catalog,
    target_schema,
    target_table
):
    path = get_latest_type3(f"{source_path}")
    df_staging = spark.read.format("delta").load(path)
    merge_condition = f_get_merge_condition(df_staging)
    df_main = DeltaTable.forName(
        spark, f"{target_catalog}.{target_schema}.{target_table}"
    )
    df_main.alias("tgt").merge(
        df_staging.alias("src"), f"{merge_condition}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

def f_get_merge_condition(df):
    import mack
    primary_key = mack.find_composite_key_candidates(df)
    merge_condition = ""
    for i in range(len(primary_key)):
        if i == (len(primary_key)-1) :
            merge_condition = merge_condition+"tgt."+primary_key[i]+" = "+"src."+primary_key[i]
        else:
            merge_condition = merge_condition+"tgt."+primary_key[i]+" = "+"src."+primary_key[i] +" AND "
    return merge_condition

# COMMAND ----------

def f_get_file_count(df):
    try:
        count = df.count()
        if count <= 100000:
            num_files = 1
        elif count > 100000 and count <= 200000:
            num_files = 2
        elif count > 200000 and count <= 300000:
            num_files = 3
        elif count > 300000 and count <= 400000:
            num_files = 4
        elif count > 400000 and count <= 500000:
            num_files = 5
        return num_files
    except Exception as e:
        print("Error Occured: ", str(e))

# COMMAND ----------

def f_optimize_zorder(filePath,orderColumnsList):
    deltaTable = DeltaTable.forPath(spark,f'{filePath}')
    deltaTable.optimize().executeZOrderBy(orderColumnsList)


# COMMAND ----------

def f_vacuum_table(filePath):
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    deltaTable = DeltaTable.forPath(spark,f'{filePath}')
    deltaTable.vacuum(0)
