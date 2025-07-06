import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime

object Main {
  def main(args: Array[String]): Unit = {
    // Инициация спарка
    val spark = SparkSession.builder()
      .appName("Hw - count Taxi Hour")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    // решение с рдд
    val sc = spark.sparkContext
    val pathTripData = getClass.getResource("/csv/tripdata.csv").getPath
    val pathTaxiZone = getClass.getResource("/csv/taxi_zone_lookup.csv").getPath

    import spark.implicits._
    println("RDD")
    loadCsvAsRdd(sc, pathTripData).toDF().show(false)
    //чтение в рдд
    val tripDataRdd = loadCsvAsRdd(sc, pathTripData)
      .map(field => {
        val hour = LocalDateTime.parse(field(1)).getHourOfDay
        val puLocationId = field(7)
        (puLocationId, hour)
      })

    val taxiZonelRdd = loadCsvAsRdd(sc, pathTaxiZone)
      .map(field => (field(0), field(1)))

    val rddResult = tripDataRdd.join(taxiZonelRdd)
      .map{case (_, (hour, borough)) => ((borough, hour), 1)}
      .reduceByKey(_ + _)
      .sortBy{ case ((borough, hour), _) => (hour, borough)}
      .map{ case ((borough, hour), countTrip) => s"$borough, $hour, $countTrip"}

    rddResult.toDF().show(false)

    println("DATAFRAME")
    //решение с датафреймом
    // Чтение данных из файла
    val tripDataDf = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .csv(pathTripData)
      .withColumn("hour", lit(hour(to_timestamp(col("tpep_pickup_datetime")))))

    val taxiZoneLDf = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .csv(pathTaxiZone)

    tripDataDf.show(false)
    taxiZoneLDf.show(false)

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

  def loadCsvAsRdd(sc: SparkContext, path:String)={
    val rdd = sc.textFile(path)
    val header = rdd.first()

    rdd.filter(_ != header)
      .map(l => l.split(",", -1).map(_.trim))
  }
}