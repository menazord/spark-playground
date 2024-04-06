package io.cristianmeneses.spark.flight

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class FlightData {

  private var schema = StructType(Array(
    StructField("date", StringType, nullable = true),
    StructField("delay", IntegerType, nullable = true),
    StructField("distance", IntegerType, nullable = true),
    StructField("origin", StringType, nullable = true),
    StructField("destination", StringType, nullable = true)))

  def run() = {

    val flightDataFile = "/Users/cristian/Downloads/departuredelays.csv"

    val spark = createSession()

    val df = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .load(flightDataFile)

    val c = df.count();
    println("Done loading " + c + " records.")

    // create temp view
    df.createOrReplaceTempView("us_delay_flights_tbl")
    println("Temp view created.")

    // more than 1k miles
    spark.sql(
        "SELECT distance, origin, destination " +
          "FROM us_delay_flights_tbl " +
          "WHERE distance > 1000 " +
          "ORDER BY distance DESC")
      .show(10)

    // SFO to ORD with 2+ hours delay
    spark.sql(
        "SELECT date, delay, origin, destination " +
          "FROM us_delay_flights_tbl " +
          "WHERE origin = 'SFO' " +
          "AND destination = 'ORD' " +
          "AND delay > 120 " +
          "ORDER BY delay DESC")
      .show(10)

    // CASE
    spark.sql(
      " SELECT delay, origin, destination, " +
      "   CASE " +
        "     WHEN delay > 360 THEN 'Very Long Delay' " +
        "     WHEN delay >= 120 AND delay <= 360 THEN 'Long Delay' " +
        "     WHEN delay > 60 AND delay < 120 THEN 'Short Delay' " +
        "     WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delay' " +
        "     WHEN delay = 0 THEN 'No Delay'" +
        " END AS Flight_Delays " +
      "  FROM us_delay_flights_tbl " +
      " ORDER BY origin, delay DESC")
      .show(10)



  }

  def createSession() : SparkSession = {

    var sparkSession = SparkSession.builder()
      .master("local")
      .appName("FlightData")
      .getOrCreate();

    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession


  }

}
