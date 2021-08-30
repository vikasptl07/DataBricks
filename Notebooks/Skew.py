# Databricks notebook source

fundamentals=spark.table('fundamentals_csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC --fundamentals=fundamentals.repartition(6,'Ticker symbol')
# MAGIC --#prices=prices.repartition(6,'symbol')
# MAGIC --#prices.select('symbol').distinct().show()
# MAGIC 
# MAGIC --update prices1 set symbol='AAP' where symbol not in (
# MAGIC --'FTV','WLTW','CSRA','HPE','KHC')
# MAGIC select symbol,count(*) from prices2 group by symbol

# COMMAND ----------

def delete_mounted_dir(dirname):
  files=dbutils.fs.ls(dirname)
  for f in files:
    if f.isDir():
      delete_mounted_dir(f.path)
    dbutils.fs.rm(f.path, recurse=True)

# COMMAND ----------


#dbutils.fs.rm('/mnt/dev/test/prices',True)


# COMMAND ----------

prices=spark.table('prices1')
delete_mounted_dir('/mnt/dev/test/prices')
prices.write.mode('overwrite').format('delta').options(path='/mnt/dev/test/prices/').partitionBy('symbol').save()
#prices=spark.table('prices1')

# COMMAND ----------

pricesdelta=spark.read.format('delta').options(path='/mnt/dev/test/prices',header=True).load()
display(pricesdelta)
pricesdelta.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import *

pricesdelta.withColumn("partitionId", spark_partition_id())\
    .groupBy("partitionId")\
    .count().show()

# COMMAND ----------

#prices=spark.table('prices1')
prices=prices.rdd.getNumPartitions()

# COMMAND ----------

display(fundamentals.limit(5))

# COMMAND ----------

display(prices.limit(5))

# COMMAND ----------

from pyspark.sql.functions import *
new=prices.alias('p').join(broadcast(fundamentals.alias('f')),col('f.Ticker Symbol')==col('p.symbol'),'inner').withColumn('totalvolume',prices.volume.cast('int'))


# COMMAND ----------

#new=new.repartition(20,'symbol')
new.rdd.getNumPartitions()

# COMMAND ----------

new.withColumn("partitionId", spark_partition_id())\
    .groupBy("partitionId")\
    .count().show()

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.skewJoin.enabled')

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.skewJoin.enabled','true')
from pyspark.sql.window import Window
new.alias('n').join(broadcast(prices.alias('p')),col('n.symbol')==col('p.symbol'),'inner').groupBy('p.symbol').sum('n.totalvolume').explain()

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/flights/

# COMMAND ----------

codes=spark.read.csv('/databricks-datasets/flights/airport-codes-na.txt',sep='\t',header=True)

# COMMAND ----------

from pyspark.sql.functions import *
schema='`date` string,`delay` INT,`distance` INT,`origin` string,`destination` string'
df=spark.read.csv('/databricks-datasets/flights/departuredelays.csv',header=True,schema=schema).withColumn('date1',from_unixtime('date','yyyy-MM-dd')).drop('date')
df=df.repartition(20,'destination')

# COMMAND ----------

df=df.repartition(5,'destination')
codes=codes.repartition(2,'IATA')
df.withColumn('partitionid',spark_partition_id()).groupBy('partitionid').count().show()

# COMMAND ----------



# COMMAND ----------

display(df.limit(5))
display(codes.limit(5))

# COMMAND ----------

df.join(codes,df.origin==codes.IATA,'inner').groupBy('date1','country','origin').\
agg(avg('delay').alias('avg_delay'),sum('distance').alias('total_distance'),avg('distance').alias('avg_distance')).show()

# COMMAND ----------


df.join(codes,df.origin==codes.IATA,'inner').groupBy('date1','country','origin').\
agg(round(avg('delay'),4).alias('avg_delay'),sum('distance').alias('total_distance'),avg('distance').alias('avg_distance')).orderBy('date1',ascending=False).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC var df1 = Seq((1,"a"),(2,"b"),(1,"c"),(1,"x"),(1,"y"),(1,"g"),(1,"k"),(1,"u"),(1,"n")).toDF("ID","NAME") 
# MAGIC 
# MAGIC df1.createOrReplaceTempView("fact")
# MAGIC 
# MAGIC var df2 = Seq((1,10),(2,30),(3,40)).toDF("ID","SALARY")
# MAGIC 
# MAGIC df2.createOrReplaceTempView("dim")
# MAGIC 
# MAGIC val salted_df1 = spark.sql("""select concat(ID, '_', FLOOR(RAND(123456)*19)) as salted_key, NAME from fact """)
# MAGIC 
# MAGIC salted_df1.createOrReplaceTempView("salted_fact")
# MAGIC 
# MAGIC val exploded_dim_df = spark.sql(""" select ID, SALARY, explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)) as salted_key from dim""")
# MAGIC 
# MAGIC //val exploded_dim_df = spark.sql(""" select ID, SALARY, explode(array(0 to 19)) as salted_key from dim""")
# MAGIC 
# MAGIC exploded_dim_df.createOrReplaceTempView("salted_dim")
# MAGIC 
# MAGIC val result_df = spark.sql("""select split(fact.salted_key, '_')[0] as ID, dim.SALARY 
# MAGIC             from salted_fact fact 
# MAGIC             LEFT JOIN salted_dim dim 
# MAGIC             ON fact.salted_key = concat(dim.ID, '_', dim.salted_key) """)
# MAGIC display(result_df)

# COMMAND ----------

from pyspark.sql.functions import *
data=((1,'a'),(2,'b'),(1,'c'),(1,'d'),(1,'e'),(1,'f'),(1,'g'))
fact=spark.createDataFrame(data,'Id int,Name string')
fact=fact.withColumn('NewId',concat('id',lit('_'),floor(rand(12345)*5)))
fact.show()

# COMMAND ----------

from pyspark.sql.types import *
dimdata=((1,100),(2,200),(3,300))
dim=spark.createDataFrame(dimdata,'Id int,salary Int')
def rangeseq():
  return list(range(0,5))
rangeudf=udf(rangeseq,ArrayType(IntegerType()))
dim=dim.withColumn('range',explode(rangeudf())).withColumn('NewID',concat('Id',lit('_'),'range'))

# COMMAND ----------

fact.alias('f').join(dim.alias('d'),col('f.NewId')==col('d.NewID'),'inner').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim

# COMMAND ----------

display(spark.sql('select * from salted_fact'))
display(spark.sql('select * from salted_dim'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select floor(rand(123456)*19)

# COMMAND ----------

spark.sql(query).show()

# COMMAND ----------

pricesdelta.select(floor(rand(123456)*19)).distinct().show()

# COMMAND ----------


