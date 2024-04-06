package io.cristianmeneses.spark.joins

import io.cristianmeneses.spark.SparkJob
import io.cristianmeneses.spark.SparkJob.spark.implicits._
import org.apache.spark.sql.SaveMode

import scala.util.Random

class ShuffleSortMergeJoinEnhanced extends SparkJob {

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

    // Save data in Parquet format, clustered
    userDF.orderBy("uid")
      .write.format("parquet")
      .bucketBy(8, "uid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("UsersTbl")

    ordersDF.orderBy("users_id")
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    // Cache the tables
    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    // Read them back in
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    val joinUsersOrdersBucketsDF = ordersBucketDF.join(usersBucketDF, $"users_id" === $"uid")

    joinUsersOrdersBucketsDF.show(false)

    joinUsersOrdersBucketsDF.explain()

    // Join
    val userOrdersDF = ordersDF.join(userDF, $"users_id" === $"uid")

    userOrdersDF.show(false)

    userOrdersDF.explain()
  }
}
