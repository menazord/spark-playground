package io.cristianmeneses.spark.udf

object Runner {

  def main(args: Array[String]): Unit = {
    val u = new UserDefinedFunctions
    u.run()
  }

}
