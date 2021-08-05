# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_schema= StructType([StructField('Id',IntegerType()),StructField('First',StringType()),
                        StructField('Last',StringType()),StructField('Url',StringType()),StructField('Published',StringType()),
                        StructField('Hits',IntegerType()),StructField('Campaigns',ArrayType(StringType()))])


df=spark.read.json('/mnt/dev/test/blogs.json',schema=json_schema)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df1=df.select(col('*'),to_date(regexp_replace(col('Published'),'/','-'),'M-d-yyyy').alias('Published_date'),explode(df.Campaigns).alias('camp')).drop(col('Campaigns')).drop(col('Published'))


# COMMAND ----------

display(df1)

# COMMAND ----------

df1=df1.select(col('*'),regexp_replace(col('camp'),'LinkedIn','twitter').alias('camp1')).drop(col('camp'))

# COMMAND ----------

df1.groupBy(col('camp1')).agg(countDistinct('Id').alias('cd')).show()

# COMMAND ----------

df1.select(countDistinct('Id')).show()

# COMMAND ----------

df.createOrReplaceTempView('blogs')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from blogs 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, transform(Campaigns ,t->concat(t,t)) as t from blogs

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets//learning-spark-v2/iot-devices/'

# COMMAND ----------

loans=spark.read.parquet('/databricks-datasets//learning-spark-v2/loans/loan-risks.snappy.parquet')

# COMMAND ----------

loans.count()

# COMMAND ----------

loans.show()

# COMMAND ----------

iot=spark.read.json('/databricks-datasets//learning-spark-v2/iot-devices/',schema=None)

# COMMAND ----------

display(iot)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
iot=iot.withColumn('Date',from_unixtime((col('timestamp')/1000))).drop('timestamp')

# COMMAND ----------

server_name = "vikasdev.database.windows.net"
database_name = "vikasdev"
jdbcPort=1433

table_name = "iot"
username = "vikasptl07"
password = "007Vik@s"


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(server_name, jdbcPort, database_name, username, password)

#iot.write.jdbc(jdbcUrl,table=table_name,mode='overwrite')
iot1= spark.read.jdbc(jdbcUrl,table=table_name)


# COMMAND ----------

"jdbc:sqlserver://{}:{};database{3};user={4};password={4}".format(server,port,username,password)
"jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}"
"jdbc:sqlserver://{0}:{1};database={};user={};password={}"


# COMMAND ----------

# MAGIC %fs ls '/iot/parqet/'

# COMMAND ----------

#dbutils.fs.rm('/iot/',True)
iot1.write.mode('overwrite').format('csv').options(header=True).save('/iot/parqet/')
#iot1.write.csv()
#iot.write.format('delta').mode('overwrite').saveAsTable('iot2')
#iot1.write.format('delta').mode('overwrite').saveAsTable('iot3')

# COMMAND ----------

spark.read.csv(path='/iot/parqet/',header=True).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO iot2 as h
# MAGIC USING iot3 as n
# MAGIC ON h.device_id=n.device_id
# MAGIC when matched then
# MAGIC update  set humidity=n.humidity 
# MAGIC when not matched then
# MAGIC insert (device_id,humidity) values(n.device_id,n.humidity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot3 order by device_id

# COMMAND ----------

iot1.write.mode('overwrite').format('delta').save('/iot/')

# COMMAND ----------

iot=iot.select('*',(col('humidity')+10).alias('humidity_1'))

# COMMAND ----------

iot.write.mode('overwrite').format('delta').save('/iot_new/')

# COMMAND ----------

t=spark.read.format('delta').load('/iot/')

# COMMAND ----------

t1=spark.read.format('delta').load('/iot_new/')

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

from delta.tables import *
iot = DeltaTable.forPath(spark, '/iot/')

# COMMAND ----------


iot1 = DeltaTable.forPath(spark, '/iot_new/')

# COMMAND ----------

MERGE INTO iot1 as n
USING iot as h
ON h.device_id=n.device_id
WHEN MATCHED 
THEN 
