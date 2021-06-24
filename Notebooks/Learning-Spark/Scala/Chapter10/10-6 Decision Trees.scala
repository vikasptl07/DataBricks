// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Decision Trees
// MAGIC 
// MAGIC In the previous notebook, you were working with the parametric model, Linear Regression. We could do some more hyperparameter tuning with the linear regression model, but we're going to try tree based methods and see if our performance improves.

// COMMAND ----------

val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
val airbnbDF = spark.read.parquet(filePath)
val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

// COMMAND ----------

// MAGIC %md
// MAGIC ## How to Handle Categorical Features?
// MAGIC 
// MAGIC We saw in the previous notebook that we can use StringIndexer/OneHotEncoderEstimator/VectorAssembler or RFormula.
// MAGIC 
// MAGIC **However, for decision trees, and in particular, random forests, we should not OHE our variables.**
// MAGIC 
// MAGIC Why is that? Well, how the splits are made is different (you can see this when you visualize the tree) and the feature importance scores are not correct.
// MAGIC 
// MAGIC For random forests (which we will discuss shortly), the result can change dramatically. So instead of using RFormula, we are going to use just StringIndexer/VectorAssembler.

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
val indexOutputCols = categoricalCols.map(_ + "Index")

val stringIndexer = new StringIndexer()
  .setInputCols(categoricalCols)
  .setOutputCols(indexOutputCols)
  .setHandleInvalid("skip")

// COMMAND ----------

// MAGIC %md
// MAGIC ## VectorAssembler
// MAGIC 
// MAGIC Let's use the VectorAssembler to combine all of our categorical and numeric inputs [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler).

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

// Filter for just numeric columns (and exclude price, our label)
val numericCols = trainDF.dtypes.filter{ case (field, dataType) => 
  dataType == "DoubleType" && field != "price"}.map(_._1)
// Combine output of StringIndexer defined above and numeric columns
val assemblerInputs = indexOutputCols ++ numericCols
val vecAssembler = new VectorAssembler()
  .setInputCols(assemblerInputs)
  .setOutputCol("features")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Decision Tree
// MAGIC 
// MAGIC Now let's build a `DecisionTreeRegressor` with the default hyperparameters [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.DecisionTreeRegressor).

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressor

val dt = new DecisionTreeRegressor().setLabelCol("price")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fit Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline

// Combine stages into pipeline
val stages = Array(stringIndexer, vecAssembler, dt)
val pipeline = new Pipeline()
  .setStages(stages)

// Uncomment to perform fit
// val pipelineModel = pipeline.fit(trainDF) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## maxBins
// MAGIC 
// MAGIC What is this parameter [maxBins](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor.maxBins)? Let's take a look at the PLANET implementation of distributed decision trees (which Spark uses) and compare it to this paper called [Yggdrasil](https://cs.stanford.edu/~matei/papers/2016/nips_yggdrasil.pdf) by Matei Zaharia and others. This will help explain the `maxBins` parameter.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/DistDecisionTrees.png" height=500px>

// COMMAND ----------

// MAGIC %md
// MAGIC In Spark, data is partitioned by row. So when it needs to make a split, each worker has to compute summary statistics for every feature for  each split point. Then these summary statistics have to be aggregated (via tree reduce) for a split to be made.
// MAGIC 
// MAGIC Think about it: What if worker 1 had the value `32` but none of the others had it. How could you communicate how good of a split that would be? So, Spark has a maxBins parameter for discretizing continuous variables into buckets, but the number of buckets has to be as large as the number of categorical variables.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and increase maxBins to `40`.

// COMMAND ----------

dt.setMaxBins(40)

// COMMAND ----------

// MAGIC %md
// MAGIC Take two.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val stages = Array(stringIndexer, vecAssembler, dt)
val pipeline = new Pipeline()
  .setStages(stages)

val pipelineModel = pipeline.fit(trainDF) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Visualize the Decision Tree

// COMMAND ----------

val dtModel = pipelineModel.stages.last
  .asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
println(dtModel.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Feature Importance
// MAGIC 
// MAGIC Let's go ahead and get the fitted decision tree model, and look at the feature importance scores.

// COMMAND ----------

val dtModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
dtModel.featureImportances

// COMMAND ----------

// MAGIC %md
// MAGIC ## Interpreting Feature Importance
// MAGIC 
// MAGIC Hmmm... it's a little hard to know what feature 4 vs 11 is. Given that the feature importance scores are "small data", let's use Pandas to help us recover the original column names.

// COMMAND ----------

val dtModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
val featureImp = vecAssembler.getInputCols.zip(dtModel.featureImportances.toArray)
val columns = Array("feature", "Importance")
val featureImpDF = spark.createDataFrame(featureImp).toDF(columns: _*)

featureImpDF.orderBy($"Importance".desc).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Apply model to test set

// COMMAND ----------

import org.apache.spark.sql.functions._

val predDF = pipelineModel.transform(testDF)

display(predDF.select("features", "price", "prediction").orderBy(desc("price")))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pitfall
// MAGIC 
// MAGIC What if we get a massive Airbnb rental? It was 20 bedrooms and 20 bathrooms. What will a decision tree predict?
// MAGIC 
// MAGIC It turns out decision trees cannot predict any values larger than they were trained on. The max value in our training set was $10,000, so we can't predict any values larger than that (or technically any values larger than the )

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val regressionEvaluator = new RegressionEvaluator()
                              .setPredictionCol("prediction")
                              .setLabelCol("price")
                              .setMetricName("rmse")

val rmse = regressionEvaluator.evaluate(predDF)
val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
println(s"RMSE is $rmse")
println(s"R2 is $r2")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Uh oh!
// MAGIC 
// MAGIC This model is worse than the linear regression model.
// MAGIC 
// MAGIC In the next few notebooks, let's look at hyperparameter tuning and ensemble models to improve upon the performance of our singular decision tree.
