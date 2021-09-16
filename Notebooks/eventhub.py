# Databricks notebook source
connectionString="Endpoint=sb://vikasdev.servicebus.windows.net/;SharedAccessKeyName=dev;SharedAccessKey=jGQYeMALiIN9ko4lurS7jKVjA1Lumjt9N+Y/XUg12kY=;EntityPath=dev"
# Initialize event hub config dictionary with connectionString
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
 
# Add consumer group to the ehConf dictionary
ehConf['eventhubs.consumerGroup'] = "$Default"
 
# Encrypt ehConf connectionString property
ehConf['eventhubs.connectionString'] =connectionString
df1 = spark.readStream.format("eventhubs").options(**ehConf).load()
 
# Visualize the Dataframe in realtime
df1 = df1.withColumn("body", df1["body"].cast("string"))
display(df1)

# COMMAND ----------

display(df)

# COMMAND ----------


