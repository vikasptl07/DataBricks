# Databricks notebook source
# MAGIC %md
# MAGIC # Basic Operations
# MAGIC 
# MAGIC This lecture will cover some basic operations with Spark DataFrames.
# MAGIC 
# MAGIC We will play around with some stock data from Apple.

# COMMAND ----------

import findspark
findspark.init('C:\Spark\spark-2.4.3-bin-hadoop2\spark-2.4.3-bin-hadoop2.7')

# COMMAND ----------

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars C:\\Spark\\spark-2.4.3-bin-hadoop2\\spark-2.4.3-bin-hadoop2.7\\jars\\spark-xml_2.11-0.10.0.jar pyspark-shell'
    

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# May take awhile locally
spark = SparkSession.builder.appName("test").getOrCreate()

# COMMAND ----------

from pyspark.sql.types import *
from decimal import Decimal

# COMMAND ----------

customSchema = StructType([ 
    StructField("category", StringType(), True), 
    StructField("author",  ArrayType(StringType()), True), 
    StructField("title", StringType(), True), 
    StructField("year", StringType(), True), 
    StructField("price", DoubleType(), True) 
    ])

# COMMAND ----------

# Let Spark know about the header and infer the Schema types!
df = spark.read.csv('appl_stock.csv',inferSchema=True,header=True)

# COMMAND ----------

df.createOrReplaceTempView('stocks')

# COMMAND ----------

spark.catalog.listTables()


# COMMAND ----------

spark.stop()

# COMMAND ----------

spark.sql('select * from stocks limit 10').show()

# COMMAND ----------

def addition(s):
    s=s*100
    return s




# COMMAND ----------

addition(2)
#print(Decimal('100.05'))

# COMMAND ----------

additionUdf = udf(lambda z: int(z)*int(100.02), StringType())
spark.udf.register("additionUdf", additionUdf)

# COMMAND ----------

spark.sql('select open,Volume,additionUdf(Volume),123432400/100 as test from stocks limit 10').show()

# COMMAND ----------

from pyspark.sql import HiveContext
hive_context = HiveContext(spark)
table=hive_context.table("stocks") 
table.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering Data
# MAGIC 
# MAGIC A large part of working with DataFrames is the ability to quickly filter out data based on conditions. Spark DataFrames are built on top of the Spark SQL platform, which means that is you already know SQL, you can quickly and easily grab that data using SQL commands, or using the DataFram methods (which is what we focus on in this course).

# COMMAND ----------

# Using SQL
df.filter("Close<500").show()

# COMMAND ----------

# Using SQL with .select()
df.filter("Close<500").select('Open').show()

# COMMAND ----------

# Using SQL with .select()
df.filter("Close<500").select(['Open','Close']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Using normal python comparison operators is another way to do this, they will look very similar to SQL operators, except you need to make sure you are calling the entire column within the dataframe, using the format: df["column name"]
# MAGIC 
# MAGIC Let's see some examples:

# COMMAND ----------

df.filter(df["Close"] < 200).show()

# COMMAND ----------

# Will produce an error, make sure to read the error!
df.filter(df["Close"] < 200 and df['Open'] > 200).show()

# COMMAND ----------

# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) & (df['Open'] > 200) ).show()

# COMMAND ----------

# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) | (df['Open'] > 200) ).show()

# COMMAND ----------

# Make sure to add in the parenthesis separating the statements!
df.filter( (df["Close"] < 200) & ~(df['Open'] < 200) ).show()

# COMMAND ----------

df.filter(df["Low"] == 197.16).show()

# COMMAND ----------

# Collecting results as Python objects
df.filter(df["Low"] == 197.16).collect()

# COMMAND ----------

result = df.filter(df["Low"] == 197.16).collect()

# COMMAND ----------

# Note the nested structure returns a nested row object
type(result[0])

# COMMAND ----------

row = result[0]

# COMMAND ----------

# MAGIC %md
# MAGIC Rows can be called to turn into dictionaries

# COMMAND ----------

row.asDict()

# COMMAND ----------

for item in result[0]:
    print(item)

# COMMAND ----------

# MAGIC %md
# MAGIC That is all for now Great Job!
