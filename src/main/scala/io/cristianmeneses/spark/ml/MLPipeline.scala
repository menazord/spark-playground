package io.cristianmeneses.spark.ml

import io.cristianmeneses.spark.SparkJob
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression

class MLPipeline extends SparkJob {

  def run() : Unit = {

    val filePath = "/Users/cristian/Downloads/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"

    val airbnbDF = spark.read.parquet(filePath)

    airbnbDF
      .select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms", "number_of_reviews", "price")
      .show(5)

    // Split train/test data, 80/20
    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)
    println(f"There are ${trainDF.count} rows in the training set and ${testDF.count} in the test set.")

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")
    val oheOutputCols = categoricalCols.map(_ + "OHE")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    val numericCols = trainDF.dtypes
      .filter{ case (field, dataType) => dataType == "DoubleType" && field != "price"}
      .map(_._1)

    val assemblerInputs = oheOutputCols ++  numericCols

    // Preparing features
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Linear regression
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    val pipeline = new Pipeline().setStages(Array(stringIndexer, oheEncoder, vecAssembler, lr))
    val pipelineModel = pipeline.fit(trainDF)

    val predDF = pipelineModel.transform(testDF)
    predDF.select("features", "price", "prediction").show(10)

    // Evaluate model predictions using RMSE
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    println(f"RMSE is $rmse%.1f")

    val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    println(s"R2 is $r2")

    val pipelinePath = "/tmp/lr-pipeline-model"
    pipelineModel.write.overwrite().save(pipelinePath)
  }

}
