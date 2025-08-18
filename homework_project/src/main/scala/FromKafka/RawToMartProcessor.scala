package FromKafka

import SendToKafka.model.NormalizeOrderModel
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
class RawToMartProcessor(spark: SparkSession) {
  // Определяем схему для Parquet файлов
  private val rawSchema = StructType(Array(
    StructField("value", StringType, true)
  ))

  def process(topic: String): Unit = {
    val rawDF = spark.read
      .schema(rawSchema)
      .parquet(s"output/raw_$topic")

    // Парсим JSON данные из поля value
    val parsedDF = parseJsonData(rawDF)

    //parsedDF.show(false)

    // 1. Заказы по часам
    buildOrdersByHour(parsedDF, s"output/orders_by_hour")

    // 2. Выручка по районам
    buildRevenueByDistrict(parsedDF, s"output/revenue_by_district")

    // 3. KPI метрики
    buildKPIMetrics(parsedDF, s"output/kpi_metrics")
  }

  private def parseJsonData(rawDF: DataFrame): DataFrame = {
    val kafkaMessageEncoder = Encoders.product[NormalizeOrderModel]

    rawDF
      .select(from_json(col("value"), kafkaMessageEncoder.schema).as("data"))
      .select("data.*")
      .na.fill("", Seq("order_id", "restaurant_id", "district", "city"))
      .na.fill(0.0, Seq("bill_amount", "total_amount", "distance_km", "rating"))
      .na.fill(0, Seq("prep_time_mins", "rider_wait_mins"))
  }

  private def buildOrdersByHour(df: DataFrame, path: String): Unit = {
    df.withColumn("hour", hour(to_timestamp(col("order_datetime"))))
      .groupBy("hour")
      .agg(
        count("*").alias("order_count"),
        sum("total_amount").alias("revenue")
      )
      .orderBy("hour")
      .write.format("orc").mode("overwrite").save(path)
  }

  private def buildRevenueByDistrict(df: DataFrame, path: String): Unit = {
    df.groupBy("district", "city")
      .agg(
        sum("total_amount").alias("revenue"),
        avg("distance_km").alias("avg_distance")
      )
      .orderBy(desc("revenue"))
      .write.partitionBy("city").format("orc").save(path)
  }

  private def buildKPIMetrics(df: DataFrame, path: String): Unit = {
    import spark.implicits._

    // Вычисляем KPI метрики
    val avgOrderValue = df.select(avg("total_amount")).first().getDouble(0)
    val avgDeliveryTime = df.select(avg("prep_time_mins")).first().getDouble(0)
    val avgRating = df.select(avg("rating")).first().getDouble(0)
    val totalOrders = df.count()
    val totalRevenue = df.select(sum("total_amount")).first().getDouble(0)

    // Создаем DataFrame с KPI метриками
    val kpiData = Seq(
      ("avg_order_value", avgOrderValue, "Средний чек"),
      ("avg_delivery_time", avgDeliveryTime, "Среднее время доставки (мин)"),
      ("avg_rating", avgRating, "Средний рейтинг"),
      ("total_orders", totalOrders.toDouble, "Общее количество заказов"),
      ("total_revenue", totalRevenue, "Общая выручка"),
      ("avg_order_per_hour", (totalOrders / 24.0), "Среднее количество заказов в час")
    )

    val kpiDF = kpiData.toDF("metric_id", "value", "metric_name")
      .withColumn("date", current_date())

    kpiDF.write.format("orc").mode("overwrite").save(path)
  }
}