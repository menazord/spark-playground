package io.cristianmeneses.spark.fire

object Runner {

  def main(args: Array[String]): Unit = {
    val f = new FireIncidents
    f.run()
  }

}
