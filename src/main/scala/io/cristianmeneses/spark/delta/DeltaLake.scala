package io.cristianmeneses.spark.delta

import io.cristianmeneses.spark.SparkJob
import io.cristianmeneses.spark.SparkJob.spark.implicits._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{AnalysisException, SaveMode}

class DeltaLake extends SparkJob {

  def run() : Unit = {

    val sourcePath = "/Users/cristian/Downloads/loan-risks.snappy.parquet"

    val deltaPath = "/Users/cristian/loans_delta"

    // Create delta table from Parquet data
    deltaSpark
      .read
      .format("parquet")
      .load(sourcePath)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(deltaPath)

    // Create a view from delta data
    deltaSpark
      .read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    // Query delta data
    deltaSpark.sql("SELECT count(*) FROM loans_delta").show()

    deltaSpark.sql("SELECT * FROM loans_delta LIMIT 5").show()

    // test for schema enforcement
    try {
      val loanUpdates = Seq(
        (1111111L, 1000, 1000.0, "TX", false),
        (2222222L, 2000, 0.0, "CA", true))
        .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

      loanUpdates.write.format("delta").mode(SaveMode.Append).save(deltaPath)
    } catch {
      case e: AnalysisException => println(e)
    }

    // Delta history
    val deltaTable = DeltaTable.forPath(deltaPath)

    deltaTable.history().show()

  }


}
