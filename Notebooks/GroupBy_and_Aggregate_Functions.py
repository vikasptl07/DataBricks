# Databricks notebook source
# MAGIC %md
# MAGIC # GroupBy and Aggregate Functions
# MAGIC 
# MAGIC Let's learn how to use GroupBy and Aggregate methods on a DataFrame. GroupBy allows you to group rows together based off some column value, for example, you could group together sales data by the day the sale occured, or group repeast customer data based off the name of the customer. Once you've performed the GroupBy operation you can use an aggregate function off that data. An aggregate function aggregates multiple rows of data into a single output, such as taking the sum of inputs, or counting the number of inputs.
# MAGIC 
# MAGIC Let's see some examples on an example dataset!

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# May take a little while on a local computer
spark = SparkSession.builder.appName("groupbyagg").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Read in the customer sales data

# COMMAND ----------

df = spark.read.csv('sales_info.csv',inferSchema=True,header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's group together by company!

# COMMAND ----------

df.groupBy("Company")

# COMMAND ----------

# MAGIC %md
# MAGIC This returns a GroupedData object, off of which you can all various methods

# COMMAND ----------

# Mean
df.groupBy("Company").mean().show()

# COMMAND ----------

# Count
df.groupBy("Company").count().show()

# COMMAND ----------

# Max
df.groupBy("Company").max().show()

# COMMAND ----------

# Min
df.groupBy("Company").min().show()

# COMMAND ----------

# Sum
df.groupBy("Company").sum().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check out this link for more info on other methods:
# MAGIC http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark-sql-module
# MAGIC 
# MAGIC Not all methods need a groupby call, instead you can just call the generalized .agg() method, that will call the aggregate across all rows in the dataframe column specified. It can take in arguments as a single column, or create multiple aggregate calls all at once using dictionary notation.
# MAGIC 
# MAGIC For example:

# COMMAND ----------

# Max sales across everything
df.agg({'Sales':'max'}).show()

# COMMAND ----------

# Could have done this on the group by object as well:

# COMMAND ----------

grouped = df.groupBy("Company")

# COMMAND ----------

grouped.agg({"Sales":'max'}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions
# MAGIC There are a variety of functions you can import from pyspark.sql.functions. Check out the documentation for the full list available:
# MAGIC http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions

# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg,stddev

# COMMAND ----------

df.select(countDistinct("Sales")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Often you will want to change the name, use the .alias() method for this:

# COMMAND ----------

df.select(countDistinct("Sales").alias("Distinct Sales")).show()

# COMMAND ----------

df.select(avg('Sales')).show()

# COMMAND ----------

df.select(stddev("Sales")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC That is a lot of precision for digits! Let's use the format_number to fix that!

# COMMAND ----------

from pyspark.sql.functions import format_number

# COMMAND ----------

sales_std = df.select(stddev("Sales").alias('std'))

# COMMAND ----------

sales_std.show()

# COMMAND ----------

# format_number("col_name",decimal places)
sales_std.select(format_number('std',2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order By
# MAGIC 
# MAGIC You can easily sort with the orderBy method:

# COMMAND ----------

# OrderBy
# Ascending
df.orderBy("Sales").show()

# COMMAND ----------

# Descending call off the column itself.
df.orderBy(df["Sales"].desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Most basic functions you would expect to be available are, so make sure to check out the documentation!
