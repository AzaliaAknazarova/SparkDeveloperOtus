import model.{AggregatedModel, TripModel, TripWithZoneModel, ZoneModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Инициация спарка
    val spark = SparkSession.builder()
      .appName("Hw - Countries")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val resourcePathCsvTaxiZone = getClass.getResource("/csv/taxi_zones.csv").getPath
    val resourcePathParquetData = getClass.getResource("/parquet/yellow_taxi_jan_25_2018").getPath

    import spark.implicits._

    val zones = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .csv(resourcePathCsvTaxiZone)
      .select($"LocationID".as("locationId"), $"Zone".as("zone"))
      .as[ZoneModel]

    val trips = spark.read
      .parquet(resourcePathParquetData)
      .select($"VendorID".as("vendorId"), $"trip_distance".as("tripDistance"), $"PULocationID".as("puLocationId"))
      .as[TripModel]

    val tripsWithZone: Dataset[TripWithZoneModel] = trips.joinWith(
      zones,
      trips("puLocationId") === zones("locationId"),
      "left"
    ).map {
      case (trip, zone) => TripWithZoneModel(
        trip.vendorId,
        trip.tripDistance,
        trip.puLocationId,
        if (zone != null) zone.zone else "unknown"
      )
    }

    val aggregatedData: Dataset[AggregatedModel] = tripsWithZone
      .groupByKey(_.zone)
      .agg(
        count("*").as[Long],
        min("tripDistance").as[Option[Double]],
        avg("tripDistance").as[Option[Double]],
        max("tripDistance").as[Option[Double]],
        stddev("tripDistance").as[Option[Double]]
      )
      .map { case (zone, count, minDist, avgDist, maxDist, stddevDist) =>
        AggregatedModel(zone, count, minDist, avgDist, maxDist, stddevDist)
      }
      .orderBy(desc("tripCount"))

    aggregatedData.show(false)

    // Сохранение результата в Parquet
    aggregatedData.write
      .mode("overwrite")
      .parquet("output/trip_zone_aggregates")

    spark.stop()
  }
}