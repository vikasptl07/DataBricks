# Databricks notebook source
k=dbutils.secrets.get(scope = "DevScope", key = "DevKey")

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://dev@vikasdevgen2.blob.core.windows.net",
mount_point = "/mnt/dev",
extra_configs = {"fs.azure.account.key.vikasdevgen2.blob.core.windows.net":dbutils.secrets.get(scope = "DevScope", key = "storagegen2")})

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "6bb15485-2745-45a0-b693-7a939b3146b4",
       "fs.azure.account.oauth2.client.secret": k,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/019ac9c9-fb87-44e3-a03d-9c3124d23552/oauth2/token",
       }

dbutils.fs.mount(
source = "abfss://dev@vikasdevgen2.dfs.core.windows.net/",
mount_point = "/mnt/dev",
extra_configs = configs)

# COMMAND ----------

#dbutils.fs.unmount('/mnt/dev')
dbutils.fs.ls("/databricks-datasets/airlines/")

# COMMAND ----------

r=spark.read.text('dbfs:/databricks-datasets/airlines/README.md')

# COMMAND ----------

r.write.text(path='/mnt/dev/README.txt')

# COMMAND ----------

df=spark.read.csv(path='dbfs:/mnt/dev/Test/data.txt',sep='\t',inferSchema=True,header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView('data')

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.groupBy().sum('amount').alias('t').show()

# COMMAND ----------


df2=df.groupBy().agg(sum("amount"),avg("amount")).select(col("sum(amount)"),col("avg(amount)")).withColumnRenamed('sum(amount)','total').withColumnRenamed('avg(amount)','totalavg')

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test (name string , amount int);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test1 (name string , amount int)
# MAGIC USING CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test2 (id string , name string)
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/dev/Test.txt'

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC --select * from  test2
# MAGIC --drop table test2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO test
# MAGIC SELECT * FROM DATA

# COMMAND ----------

# MAGIC %sql INSERT INTO test1 SELECT * FROM DATA 

# COMMAND ----------

# MAGIC %fs ls "/mnt/dev/Test/"

# COMMAND ----------

# MAGIC %fs ls "/user/hive/warehouse/test1/"

# COMMAND ----------


