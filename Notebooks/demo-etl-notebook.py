# Databricks notebook source
#declare the column name widget that Data Factory can pass values to as a parameter
dbutils.widgets.text("colName", "")

# COMMAND ----------

#set the parameter to a value
colName =  dbutils.widgets.get("colName")

# COMMAND ----------

##for debugging only
#colName = "age"

# COMMAND ----------

#create a sample dataframe, using the parameter passed in to label the column.
df = spark.createDataFrame(["10","11","13"], "string").toDF(colName)

# COMMAND ----------

#display the dataframe to view the data
display(df)

# COMMAND ----------

#declare a temp path where we will write out our data
deltaMiniDataPath = '/dbfs/delta/tmp/'

# COMMAND ----------

#write the dataframe out to the temporary location
(df
  .write
  .mode("overwrite")
  .format("delta")
  .save(deltaMiniDataPath) 
)

# COMMAND ----------

#create a table on top of the written data
spark.sql("""
    CREATE TABLE IF NOT EXISTS age
    USING DELTA 
    LOCATION '{}' 
  """.format(deltaMiniDataPath))

# COMMAND ----------

#get a count of the number of records in the table
count = spark.sql("select count(*) from age").collect()[0][0]


# COMMAND ----------

#exit the notebook and pass how many records have been processed
dbutils.notebook.exit('{} record(s) have been processed.'.format(count))
