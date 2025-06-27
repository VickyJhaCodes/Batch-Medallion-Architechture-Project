# Databricks notebook source
dbutils.widgets.text('Notebook Path','')
notebook_path = dbutils.widgets.get('Notebook Path')
dbutils.widgets.text('TableName','')
tablename = dbutils.widgets.get('TableName')

# COMMAND ----------

if tablename == '':
    dbutils.notebook.run(f'{notebook_path}',0)
else:
    dbutils.notebook.run(f'{notebook_path}',6000,{'TableName':tablename})
