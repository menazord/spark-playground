package io.cristianmeneses.spark

import org.apache.spark.sql.SparkSession

trait SparkJob {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("playground")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}

object SparkJob extends SparkJob {}

