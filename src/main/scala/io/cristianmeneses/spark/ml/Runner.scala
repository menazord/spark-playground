package io.cristianmeneses.spark.ml

object Runner {

  def main(args: Array[String]): Unit = {
    val p = new MLPipeline
    p.run()
  }

}
