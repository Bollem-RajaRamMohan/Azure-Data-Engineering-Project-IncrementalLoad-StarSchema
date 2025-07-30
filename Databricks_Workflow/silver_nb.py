# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*


# COMMAND ----------

# MAGIC %md
# MAGIC #create catalog

# COMMAND ----------

# MAGIC %md
# MAGIC #create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS deltaprjt.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS deltaprjt.gold

# COMMAND ----------

# MAGIC %md
# MAGIC ##read parquet 

# COMMAND ----------

df_sil=spark.read.format('parquet')\
        .option('inferSchema',True)\
        .load('abfss://bronze@prjtdelta.dfs.core.windows.net/rawdata')

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformations

# COMMAND ----------

df_sil=df_sil.withColumn('model_categorey',split(col('Model_ID'),'-')[0])
df_sil=df_sil.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df_sil.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@prjtdelta.dfs.core.windows.net/carsales')\
            .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet. `abfss://silver@prjtdelta.dfs.core.windows.net/carsales`