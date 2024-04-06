package io.cristianmeneses.spark.datasets

import io.cristianmeneses.spark.SparkJob
import io.cristianmeneses.spark.SparkJob.spark.implicits._
import org.apache.spark.sql.functions._

import scala.util.Random

case class Usage(uid:Int, uname:String, usage:Int)
case class UsageCost(uid:Int, uname:String, usage:Int, cost: Double)

class Datasets extends SparkJob {

  def run(): Unit = {
    val r = new Random(42)

    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    // Transforms
    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, truncate = false)

    //HOF
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750)
        usage * 0.15
      else
        usage * 0.50
    }

    dsUsage.map(u => computeCostUsage(u.usage)).show(5, truncate = false)

    // with cost
    def computeUserCostUsage(u: Usage): UsageCost = {
      if (u.usage > 750)
        UsageCost(u.uid, u.uname, u.usage, u.usage * 0.15)
      else
        UsageCost(u.uid, u.uname, u.usage, u.usage * 0.50)
    }
    dsUsage.map(u => computeUserCostUsage(u)).show(5, truncate = false)


  }
}
