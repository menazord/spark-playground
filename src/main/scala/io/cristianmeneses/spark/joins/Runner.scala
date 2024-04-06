package io.cristianmeneses.spark.joins

object Runner {

  def main(args: Array[String]): Unit = {
    val s = new ShuffleSortMergeJoin
    s.run()
  }

}
