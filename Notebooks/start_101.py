# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

#accountkey
dbutils.fs.mount(
source = "wasbs://test@skywalkerdevstg.blob.core.windows.net",
mount_point = "/mnt/dev",
extra_configs = {"fs.azure.account.key.skywalkerdevstg.blob.core.windows.net":dbutils.secrets.get(scope = "DevScope", key = "dev")})

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.mount(
# MAGIC source="wasbs://test@vikasdev.blob.core.windows.net"  ,
# MAGIC mount_point='/mnt/dev/',
# MAGIC extra_configs={"fs.azure.account.key.vikasdev.blob.core.windows.net":dbutils.secrets.get('DevScope',key='')}  
# MAGIC 
# MAGIC )
# MAGIC jdbc:sqlserver://{}:{};database;user;password
# MAGIC       
# MAGIC dbutils.fs.mount(
# MAGIC source="wasbs://test@vikasdev.blob.core.windows.net",
# MAGIC   mount_point="",
# MAGIC   extra_configs={"fs.azure.account.key.vikasdev.blob.core.windows.net":}
# MAGIC 
# MAGIC )      
# MAGIC       
# MAGIC       
# MAGIC       
# MAGIC       

# COMMAND ----------

##sas token
dbutils.fs.mount(
source = "wasbs://test@vikasdev.blob.core.windows.net",
mount_point = "/mnt/dev",
extra_configs = {"fs.azure.sas.test.vikasdev.blob.core.windows.net":dbutils.secrets.get(scope = "DevScope", key = "sas")})

# COMMAND ----------

##serviceprincipal
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

#%fs ls '/mnt/'
#dbutils.fs.unmount('/mnt/dev')
#dbutils.fs.ls("/databricks-datasets/airlines/")



# COMMAND ----------

r=spark.read.text('dbfs:/databricks-datasets/airlines/README.md')

# COMMAND ----------

df=spark.read.format('csv').options(path='dbfs:/mnt/dev/test/Employee.txt',sep='\t',inferSchema=True,header=True).load()

# COMMAND ----------

#df=spark.read.csv(path='dbfs:/mnt/dev/test/Employee.txt',sep='\t',inferSchema=True,header=True)
df.show()

# COMMAND ----------

df.write.format('csv').options(path='dbfs:/mnt/dev/test/Employee1.txt',sep='\t',header=True).mode('overwrite').save()

# COMMAND ----------

df=spark.read.csv(path='/mnt/dev/test/Employee1.txt',sep='\t',header=True,inferSchema=True)

# COMMAND ----------

#df.fillna({'_c6':'test'}).show()
df.show()

# COMMAND ----------

# MAGIC %pip install random

# COMMAND ----------

import random
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
def arraylist(cl):
  return cl+'_'+str(random.randint(1,10))
#print(arraylist1('test'))
arraylist1=udf(arraylist,StringType())
df.withColumn('tile',arraylist1('department')).show()

# COMMAND ----------

df.withColumn('tile',arraylist1('department')).show()

# COMMAND ----------

from pyspark.sql.functions import *

"""
df.withColumn('salary_desc',when(col('salary') <= 90000,'low').when(col('salary') > 90000 & col('salary') <200000   ,'Medium').when(col('salary') >200000,'high').otherwise(lit(""))).show()
"""

df.select(col("*"),when(col('salary') <= 90000,'low')\
          .when((col('salary') > 90000 ) & (col('salary') < 200000 ),'M')\
         .when(col('salary')>=200000,'h').alias('temp'),regexp_replace(col('department'),'HR','H')).show()


# COMMAND ----------

df.dtypes


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

from pyspark.sql.functions import *
df.groupBy().agg(sum('salary').alias('s'),avg('salary').alias('avg')).show()

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

dbutils.notebook.run('demo-etl-notebook',10000,{"colName":"Vikas"})

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import Row
df=spark.createDataFrame(spark.sparkContext.parallelize([Row(1)]),'Id int')

# COMMAND ----------

df.select(month(current_date())).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

Window.partitionBy()
