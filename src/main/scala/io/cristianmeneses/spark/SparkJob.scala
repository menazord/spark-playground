package io.cristianmeneses.spark

import org.apache.spark.sql.SparkSession

trait SparkJob {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("playground")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val deltaSpark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("playground")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  deltaSpark.sparkContext.setLogLevel("ERROR")
}

object SparkJob extends SparkJob {}

