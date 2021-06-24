# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Tables
# MAGIC 
# MAGIC This notebook shows how to use Spark Catalog Interface API to query databases, tables, and columns.
# MAGIC 
# MAGIC A full list of documented methods is available [here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Catalog)

# COMMAND ----------

us_flights_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Managed Tables

# COMMAND ----------

# Create database and managed tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE") 
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the databases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read our US Flights table

# COMMAND ----------

df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
      .load())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save into our table

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl_ADF")

# COMMAND ----------

# MAGIC %md
# MAGIC Check if the table is cached

# COMMAND ----------


