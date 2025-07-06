import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Инициация спарка
    val spark = SparkSession.builder()
      .appName("Hw - count Taxi Hour")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    // Чтение данных из файла
    val tripDataDf = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .csv(getClass.getResource("/csv/tripdata.csv").getPath)
      .withColumn("hour", lit(hour(to_timestamp(col("tpep_pickup_datetime")))))

    val taxiZoneLDf = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .csv(getClass.getResource("/csv/taxi_zone_lookup.csv").getPath)

    tripDataDf.show(false)
    taxiZoneLDf.show(false)

    import spark.implicits._

    val res = tripDataDf.join(taxiZoneLDf, tripDataDf("PULocationID") === taxiZoneLDf("LocationID"))
      .groupBy($"Borough", $"hour")
      .count()
      .orderBy($"hour", $"Borough")

    res.show(false)
    // сохранение результата
    res.write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .csv("output/trip_count_hours")

    spark.stop()
  }
}