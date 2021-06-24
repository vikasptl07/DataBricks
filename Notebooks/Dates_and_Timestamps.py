# Databricks notebook source
# MAGIC %md
# MAGIC # Dates and Timestamps
# MAGIC 
# MAGIC You will often find yourself working with Time and Date information, let's walk through some ways you can deal with it!

# COMMAND ----------

from pyspark.sql import SparkSession
# May take a little while on a local computer
spark = SparkSession.builder.appName("dates").getOrCreate()

# COMMAND ----------

df = spark.read.csv("appl_stock.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's walk through how to grab parts of the timestamp data

# COMMAND ----------

from pyspark.sql.functions import format_number,dayofmonth,hour,dayofyear,month,year,weekofyear,date_format

# COMMAND ----------

df.select(dayofmonth(df['Date'])).show()

# COMMAND ----------

df.select(hour(df['Date'])).show()

# COMMAND ----------

df.select(dayofyear(df['Date'])).show()

# COMMAND ----------

df.select(month(df['Date'])).show()

# COMMAND ----------

# MAGIC %md
# MAGIC So for example, let's say we wanted to know the average closing price per year. Easy! With a groupby and the year() function call:

# COMMAND ----------

df.select(year(df['Date'])).show()

# COMMAND ----------

df.withColumn("Year",year(df['Date'])).show()

# COMMAND ----------

newdf = df.withColumn("Year",year(df['Date']))
newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']].show()

# COMMAND ----------

# MAGIC %md
# MAGIC Still not quite presentable! Let's use the .alias method as well as round() to clean this up!

# COMMAND ----------

result = newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']]
result = result.withColumnRenamed("avg(Year)","Year")
result = result.select('Year',format_number('avg(Close)',2).alias("Mean Close")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Perfect! Now you know how to work with Date and Timestamp information!
