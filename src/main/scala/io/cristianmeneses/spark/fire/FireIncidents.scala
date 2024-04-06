package io.cristianmeneses.spark.fire

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, desc, rtrim, to_date, to_timestamp, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

class FireIncidents {

    private var schema = StructType(Array(
        StructField("IncidentNumber", IntegerType, nullable = true),
        StructField("ExposureNumber", IntegerType, nullable = true),
        StructField("ID", IntegerType, nullable = true),
        StructField("Address", StringType, nullable = true),
        StructField("IncidentDate", StringType, nullable = true),
        StructField("CallNumber", IntegerType, nullable = true),
        StructField("AlarmDtTm", StringType, nullable = true),
        StructField("ArrivalDtTm", StringType, nullable = true),
        StructField("CloseDtTm", StringType, nullable = true),
        StructField("City", StringType, nullable = true),
        StructField("ZipCode", IntegerType, nullable = true),
        StructField("Battalion", StringType, nullable = true),
        StructField("StationArea", StringType, nullable = true),
        StructField("Box", StringType, nullable = true),
        StructField("SuppressionUnits", IntegerType, nullable = true),
        StructField("SuppressionPersonnel", IntegerType, nullable = true),
        StructField("EMSUnits", IntegerType, nullable = true),
        StructField("EMSPersonnel", IntegerType, nullable = true),
        StructField("OtherUnits", IntegerType, nullable = true),
        StructField("OtherPersonnel", IntegerType, nullable = true),
        StructField("FirstUnitOnScene", IntegerType, nullable = true),
        StructField("EstimatedPropertyLoss", IntegerType, nullable = true),
        StructField("EstimatedContentLoss", IntegerType, nullable = true),
        StructField("FireFatalities", IntegerType, nullable = true),
        StructField("FireInjuries", IntegerType, nullable = true),
        StructField("CivilianFatalities", IntegerType, nullable = true),
        StructField("CivilianInjuries", IntegerType, nullable = true),
        StructField("NumberOfAlarms", IntegerType, nullable = true),
        StructField("PrimarySituation", StringType, nullable = true),
        StructField("MutualAid", StringType, nullable = true),
        StructField("ActionTakenPrimary", StringType, nullable = true),
        StructField("ActionTakenSecondary", StringType, nullable = true),
        StructField("ActionTakenOther", StringType, nullable = true),
        StructField("DetectorAlertedOccupants", StringType, nullable = true),
        StructField("PropertyUse", StringType, nullable = true),
        StructField("AreaOfFireOrigin", StringType, nullable = true),
        StructField("IgnitionCause", StringType, nullable = true),
        StructField("IgnitionFactorPrimary", StringType, nullable = true),
        StructField("IgnitionFactorSecondary", StringType, nullable = true),
        StructField("HeatSource", StringType, nullable = true),
        StructField("ItemFirstIgnited", StringType, nullable = true),
        StructField("HumanFactors", StringType, nullable = true),
        StructField("StructureType", StringType, nullable = true),
        StructField("StructureStatus", StringType, nullable = true),
        StructField("FloorFireOrigin", StringType, nullable = true),
        StructField("FireSpread", StringType, nullable = true),
        StructField("NoFlameSpread", StringType, nullable = true),
        StructField("NumberFloorsMinimalDamage", StringType, nullable = true),
        StructField("NumberFloorsSignificantDamage", StringType, nullable = true),
        StructField("NumberFloorsHeavyDamage", StringType, nullable = true),
        StructField("NumberFloorsExtremeDamage", StringType, nullable = true),
        StructField("DetectorsPresent", StringType, nullable = true),
        StructField("DetectorType", StringType, nullable = true),
        StructField("DetectorOperation", StringType, nullable = true),
        StructField("DetectorEffectiveness", StringType, nullable = true),
        StructField("DetectorFailureReason", StringType, nullable = true),
        StructField("AutomaticExtinguishingSystemPresent", StringType, nullable = true),
        StructField("AutomaticExtinguishingSystemType", StringType, nullable = true),
        StructField("AutomaticExtinguishingSystemPerformance", StringType, nullable = true),
        StructField("AutomaticExtinguishingSystemFailureReason", StringType, nullable = true),
        StructField("NumberSprinklerHeadsOperating", StringType, nullable = true),
        StructField("SupervisorDistrict", IntegerType, nullable = true),
        StructField("NeighborhoodDistrict", StringType, nullable = true),
        StructField("Point", StringType, nullable = true),
        StructField("data_as_of", StringType, nullable = true),
        StructField("data_loaded_at", StringType, nullable = true)))


    def run(): Unit = {

        var spark = createSession()

        val sfFireFile = "/Users/cristian/Downloads/Fire_Incidents.csv"
        val fireDF = spark.read.schema(schema).option("header", "true").csv(sfFireFile);

        val count = fireDF.count();

        println("Done reading " + count + " rows")

        // Transforms

        val fewFireDF = fireDF
          .select("IncidentNumber", "AlarmDtTm",  "PrimarySituation")
          .where(col("PrimarySituation") =!= "311 - Medical assist, assist EMS crew")
        fewFireDF.show(5, truncate = false)

        fireDF
          .select("PrimarySituation")
          .where(col("PrimarySituation").isNotNull)
          .agg(countDistinct("PrimarySituation").alias("DistinctCallTypes"))
          .show()

        fireDF
          .select("PrimarySituation")
          .where(col("PrimarySituation").isNotNull)
          .distinct()
          .show(10, truncate = false)

        // Date format conversion
        val fireDtDF = fireDF
          .withColumn("IncidentDateTS", to_timestamp(col("IncidentDate"), "yyyy/MM/dd"))
          .drop("IncidentDate")
          .withColumn("AlarmTS", to_timestamp(col("AlarmDtTm"), "yyyy/MM/dd hh:mm:ss a"))
          .drop("AlarmDtTm")

        fireDtDF
          .select("IncidentDateTS", "AlarmTS")
          .show(5, truncate = false)

        // Using time functions on converted columns
        fireDtDF
          .select(year(col("IncidentDateTS")))
          .distinct()
          .orderBy(year(col("IncidentDateTS")))
          .show()

        // Aggregation
        val fireAgg = fireDtDF
          .select("PrimarySituation")
          .where(col("PrimarySituation").isNotNull)
          .groupBy("PrimarySituation")
          .count()
          .orderBy(desc("count"))

        fireAgg.show(10, truncate = false)
        fireAgg.explain(true)


        // Parquet writer
        val sfParquetFile = "/Users/cristian/Downloads/Fire_Incidents.parquet"

        if (Files.exists(Paths.get(sfParquetFile))) {
            println("Deleting existing files at " + sfParquetFile)
            val directory = new Directory(new File(sfParquetFile))
            directory.deleteRecursively()
        }

        fireDF.write.format("parquet").save(sfParquetFile);

        println("Done saving to " + sfParquetFile + " file")

    }

    private def createSession() : SparkSession = {

        var sparkSession = SparkSession.builder()
          .master("local")
          .getOrCreate();

        sparkSession.sparkContext.setLogLevel("ERROR")

        sparkSession
    }
}
