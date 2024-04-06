package io.cristianmeneses.spark.joins

import io.cristianmeneses.spark.SparkJob
import io.cristianmeneses.spark.SparkJob.spark.implicits._

import scala.util.Random

class ShuffleSortMergeJoin extends SparkJob {

  def run(): Unit = {

    // disable broadcast joins
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new Random(42)

    states += (0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")

    // Random data
    val userDF = (0 to 1000000)
      .map(id => (id, s"user_${id}", s"user_${id}@lala.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = (0 to 1000000)
      .map(r => (r, r, rnd.nextInt(10000), 10 * r * 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    // Join
    val userOrdersDF = ordersDF.join(userDF, $"users_id" === $"uid")

    userOrdersDF.show(false)

    userOrdersDF.explain()
  }
}
