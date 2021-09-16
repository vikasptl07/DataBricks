# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

#accountkey
dbutils.fs.mount(
source = "wasbs://test@skywalkerdevstg.blob.core.windows.net",
mount_point = "/mnt/dev",
extra_configs = {"fs.azure.account.key.skywalkerdevstg.blob.core.windows.net":dbutils.secrets.get(scope = "DevScope", key = "dev")})

# COMMAND ----------

#accountkey
dbutils.fs.mount(
source = "wasbs://test@skywalkerdevstg.blob.core.windows.net",
mount_point = "/mnt/dev",
extra_configs = {"fs.azure.account.key.skywalkerdevstg.blob.core.windows.net":'VMmmfnFYcD3v1NjLdJ5l58zn4FrLwNu4Xl5LrjlrSrzEYveoe8EI1bxzedsD9iY1HEPMqLBeqWNf9BesAUTAKQ=='})

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

sample=list(range(1000))

# COMMAND ----------

from pyspark.sql import Row
sample2=spark.sparkContext.parallelize(sample).map(lambda x: Row(x))
df2=sample2.toDF(schema="id int")
df2.filter('id>=100 and id<=999').groupBy().sum('id').show()

# COMMAND ----------

sum=0
for i in sample:
  if(i>=100 and i<=999):
    sum=sum+i
print(sum)    

# COMMAND ----------

rdd2=rdd.filter(lambda x:x>=100 and x<=999).map(lambda x: (x,1))

# COMMAND ----------

demo=[("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82), 
  ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), 
  ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87), 
  ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74), 
  ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), 
  ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83), 
  ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)]

df=spark.createDataFrame(demo,"Name string,Subject string,Marks int")
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql

# COMMAND ----------

df.groupBy(col('Name')).agg(max('marks').alias('max_val')).show()

# COMMAND ----------

dbutils.widgets.dropdown("X123", "1", [str(x) for x in range(1, 10)])

# COMMAND ----------

dbutils.widgets.get("X123")

# COMMAND ----------

# MAGIC %fs ls /mnt/dev/

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import yaml
with open('/dbfs/mnt/dev/sample.yaml','r') as f:
  data = yaml.load(f, Loader=yaml.SafeLoader)
print(data)
d="""Config:
    - name: identity
      value: 101
    - name: size
      value: 201
    - name: logloc
      value: /tmp/log
    - name: host
      value: xyz.dev.cloud"""
data2=yaml.load(d,Loader=yaml.SafeLoader)
print(data2)
template = "UPDATE SAMPLE SET PVALUE={} WHERE PNAME={}"
statements = [template.format(nv['value'], nv['name']) for nv in data['Config']]

print(statements)

# COMMAND ----------


