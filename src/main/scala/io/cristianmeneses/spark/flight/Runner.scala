package io.cristianmeneses.spark.flight

object Runner {

  def main(args: Array[String]): Unit = {

    val f = new FlightData
    f.run()
  }
}
