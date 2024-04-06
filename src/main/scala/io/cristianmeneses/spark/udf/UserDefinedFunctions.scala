package io.cristianmeneses.spark.udf

import org.apache.spark.sql.SparkSession

class UserDefinedFunctions {

  def run(): Unit = {

    val spark = createSession()

    val cubed = (s: Long) => {
      s * s * s
    }

    spark.udf.register("cubed", cubed)
    println("Registered UDF")

    spark.range(1, 9).createOrReplaceTempView("udf_test")
    println("Created temp view")

    spark.sql("SELECT id, cubed(id) as id_cubed FROM udf_test").show(10)
  }

  def createSession(): SparkSession = {
      val spark = SparkSession.builder()
        .appName("UDF-Test")
        .master("local")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
  }
}



