package io.cristianmeneses.spark.joins

object RunnerEnhanced {

  def main(args: Array[String]): Unit = {
    val s = new ShuffleSortMergeJoinEnhanced
    s.run()
  }

}
