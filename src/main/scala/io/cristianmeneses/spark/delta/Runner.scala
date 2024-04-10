package io.cristianmeneses.spark.delta

object Runner {

  def main(args: Array[String]): Unit = {
    val d = new DeltaLake
    d.run()
  }

}
