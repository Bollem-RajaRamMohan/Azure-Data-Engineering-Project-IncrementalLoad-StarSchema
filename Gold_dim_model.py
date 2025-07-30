# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %md
# MAGIC # create flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get('incremental_flag')
print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Table

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql('''
select distinct(Model_ID) as Model_ID,model_categorey from parquet.`abfss://silver@prjtdelta.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###dim_model Sink initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('deltaprjt.gold.dim_model'):
    df_sink= spark.sql('''
    select dim_model_key,Model_ID,model_categorey 
    from deltaprjt.gold.dim_model
    ''')

else:

    df_sink= spark.sql('''
    select 1 as dim_model_key,Model_ID,model_categorey 
    from parquet.`abfss://silver@prjtdelta.dfs.core.windows.net/carsales`
    where 1=0
    ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###filtering old and new records

# COMMAND ----------



# COMMAND ----------

df_filter = df_src.join(df_sink,df_src.Model_ID==df_sink.Model_ID,'left').select(df_src.Model_ID,df_src.model_categorey,df_sink.dim_model_key)
df_filter.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(df_filter.dim_model_key.isNotNull())
df_filter_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###df_filter_new

# COMMAND ----------

df_filter_new=df_filter.filter(df_filter.dim_model_key.isNull()).select(df_src.Model_ID,df_src.model_categorey)

# COMMAND ----------

df_filter_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ####fetch the max surrogate key from existing table

# COMMAND ----------

if incremental_flag=='0':
    max_value=1
else:
    max_value_df=spark.sql('''
    select max(dim_model_key) from deltaprjt.gold.dim_model
    ''')
    max_value=max_value_df.collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC ####create surrogate key column and add max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####create final_df = df_filter_old+df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD - Type 1 (Upsert)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('deltaprjt.gold.dim_model'):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@prjtdelta.dfs.core.windows.net/dim_model")
    delta_tbl.alias('trg').merge(df_final.alias('src'),'trg.dim_model_key=src.dim_model_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()


else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@prjtdelta.dfs.core.windows.net/dim_model')\
        .saveAsTable('deltaprjt.gold.dim_model')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltaprjt.gold.dim_model