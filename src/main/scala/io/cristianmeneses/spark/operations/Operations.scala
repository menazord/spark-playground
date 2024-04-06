package io.cristianmeneses.spark.operations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, e, expr, grouping, sum}

class Operations {

  def run(): Unit = {

    val spark = createSession()

    val delaysPath = "/Users/cristianmeneses/Downloads/departuredelays.csv"
    val airportsPath = "/Users/cristianmeneses/Downloads/airport-codes-na.txt"

    val airports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports")
    println("Done processing airport data and view")

    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")
    println("Done processing delays data and view")

    val foo = delays.filter(
      expr("origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"))
    foo.createOrReplaceTempView("foo")
    println("Done processing foo view")

    spark.sql("SELECT * FROM airports LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    // Unions
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(
      expr("origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"))
      .show()

    // Joins
    foo.as("f").join(
      airports.as("air"),
      expr("air.IATA == f.origin")
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    // Windowing
    val departureDelaysWindow = delays
      .filter(expr("origin IN ('SEA', 'SFO', 'JFK')"))
      .filter(expr("destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')"))
      .groupBy("origin", "destination")
      .agg(sum("delay").alias("TotalDelays"))
    departureDelaysWindow.createOrReplaceTempView("departureDelaysWindow")

    spark.sql("SELECT * from departureDelaysWindow").show()

    // Worst 3 delays per origin
    spark.sql(
      "SELECT origin, destination, TotalDelays, rank " +
      "  FROM ( " +
          "SELECT origin, destination, TotalDelays, dense_rank() " +
          "  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank " +
          "  FROM departureDelaysWindow " +
        ") t " +
      " WHERE rank <= 3").show()

    // Modifications
    // Add column
    val foo2 = foo
      .withColumn(
        "status",
        expr("CASE WHEN delay <= 10 THEN 'On-Time' ELSE 'Delayed' END"))
    foo2.show()

    // Drop column
    val foo3 = foo2.drop("delay")
    foo3.show()

    // Rename column
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()
  }

  def createSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Operations-Test")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}