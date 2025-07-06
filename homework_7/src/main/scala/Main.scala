import model.{TripModel, ZoneModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
      .as[ZoneModel]

    val trips = spark.read
      .parquet(resourcePathParquetData)
      .as[TripModel]

    val tripsWithZone = trips.join(
      zones,
      trips("PULocationID") === zones("locationId"),
      "left"
    )

    val aggregatedData = tripsWithZone
      .groupBy($"zone")
      .agg(
        count("*").as("trip_count"),
        min($"trip_distance").as("min_distance"),
        avg($"trip_distance").as("avg_distance"),
        max($"trip_distance").as("max_distance"),
        stddev($"trip_distance").as("stddev_distance")
      )
      .orderBy(desc("trip_count"))

    aggregatedData.show(false)

    // Сохранение результата в Parquet
    aggregatedData.write
      .mode("overwrite")
      .parquet("output/trip_zone_aggregates")

    spark.stop()
  }
}