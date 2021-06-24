# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DataFrame Basics
# MAGIC 
# MAGIC Spark DataFrames are the workhouse and main way of working with Spark and Python post Spark 2.0. DataFrames act as powerful versions of tables, with rows and columns, easily handling large datasets. The shift to DataFrames provides many advantages:
# MAGIC * A much simpler syntax
# MAGIC * Ability to use SQL directly in the dataframe
# MAGIC * Operations are automatically distributed across RDDs
# MAGIC     
# MAGIC If you've used R or even the pandas library with Python you are probably already familiar with the concept of DataFrames. Spark DataFrame expand on a lot of these concepts, allowing you to transfer that knowledge easily by understanding the simple syntax of Spark DataFrames. Remember that the main advantage to using Spark DataFrames vs those other programs is that Spark can handle data across many RDDs, huge data sets that would never fit on a single computer. That comes at a slight cost of some "peculiar" syntax choices, but after this course you will feel very comfortable with all those topics!
# MAGIC 
# MAGIC Let's get started!
# MAGIC 
# MAGIC ## Creating a DataFrame
# MAGIC 
# MAGIC First we need to start a SparkSession:

# COMMAND ----------

python -m pip3 install findspark

# COMMAND ----------



import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

pip install findspark


# COMMAND ----------

yarn application -list


# COMMAND ----------

findspark.init('C:\Spark\spark-2.4.3-bin-hadoop2\spark-2.4.3-bin-hadoop2.7')

# COMMAND ----------

# MAGIC %md
# MAGIC Then start the SparkSession

# COMMAND ----------

# May take a little while on a local computer
spark = SparkSession.builder.appName("Basics").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You will first need to get the data from a file (or connect to a large distributed file like HDFS, we'll talk about this later once we move to larger datasets on AWS EC2).

# COMMAND ----------

# We'll discuss how to read other options later.
# This dataset is from Spark's examples

# Might be a little slow locally
df = spark.read.csv('C:\\Users\\Vikas\\Documents\\Python-and-Spark-for-Big-Data-master\\Data Set Generator (remove me the future!)\\DataSets\\hack_data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Showing the data

# COMMAND ----------

# Note how data is missing!
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Some data types make it easier to infer schema (like tabular formats such as csv which we will show later). 
# MAGIC 
# MAGIC However you often have to set the schema yourself if you aren't dealing with a .read method that doesn't have inferSchema() built-in.
# MAGIC 
# MAGIC Spark has all the tools you need for this, it just requires a very specific structure:

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StructType

# COMMAND ----------

# MAGIC %md
# MAGIC Next we need to create the list of Structure fields
# MAGIC     * :param name: string, name of the field.
# MAGIC     * :param dataType: :class:`DataType` of the field.
# MAGIC     * :param nullable: boolean, whether the field can be null (None) or not.

# COMMAND ----------

data_schema = [StructField("age", IntegerType(), True),StructField("name", StringType(), True)]

# COMMAND ----------

final_struc = StructType(fields=data_schema)

# COMMAND ----------

df = spark.read.json('people.json', schema=final_struc)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grabbing the data

# COMMAND ----------

df['age']

# COMMAND ----------

type(df['age'])

# COMMAND ----------

df.select('age')

# COMMAND ----------

type(df.select('age'))

# COMMAND ----------

df.select('age').show()

# COMMAND ----------

# Returns list of Row objects
df.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Multiple Columns:

# COMMAND ----------

df.select(['age','name'])

# COMMAND ----------

df.select(['age','name']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating new columns

# COMMAND ----------

# Adding a new column with a simple copy
df.withColumn('newage',df['age']).show()

# COMMAND ----------

df.show()

# COMMAND ----------

# Simple Rename
df.withColumnRenamed('age','supernewage').show()

# COMMAND ----------

# MAGIC %md
# MAGIC More complicated operations to create new columns

# COMMAND ----------

df.withColumn('doubleage',df['age']*2).show()

# COMMAND ----------

df.withColumn('add_one_age',df['age']+1).show()

# COMMAND ----------

df.withColumn('half_age',df['age']/2).show()

# COMMAND ----------

df.withColumn('half_age',df['age']/2)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll discuss much more complicated operations later on!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SQL
# MAGIC 
# MAGIC To use SQL queries directly with the dataframe, you will need to register it to a temporary view:

# COMMAND ----------

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# COMMAND ----------

sql_results = spark.sql("SELECT * FROM people")

# COMMAND ----------

sql_results

# COMMAND ----------

sql_results.show()

# COMMAND ----------

spark.sql("SELECT * FROM people WHERE _c1>10.0").show()

# COMMAND ----------

spark.stop


# COMMAND ----------

# MAGIC %md
# MAGIC We won't really be focusing on using the SQL syntax for this course in general, but keep in mind it is always there for you to get you out of bind quickly with your SQL skills!

# COMMAND ----------

# MAGIC %md
# MAGIC Alright that is all we need to know for now!
