package BI

import FromKafka.{KafkaToRawProcessor, RawToMartProcessor}
import org.apache.spark.sql.SparkSession

object FoodDeliveryBI {
  // Kafka
  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val KAFKA_TOPIC = "food_orders_pj"

  val RAW_PATH = "output"

  // Имена таблиц Mart
  val ORDERS_BY_HOUR = "orders_by_hour"
  val REVENUE_BY_DISTRICT = "revenue_by_district"
  val KPI_METRICS = "kpi_metrics"

  def main(args: Array[String]): Unit = {
    // Инициализация Spark
    val spark = SparkSession.builder()
      .appName("FoodDeliveryBI")
      .master("local[*]") // Явно указываем master
      .config("spark.log.level", "WARN")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.orc.impl", "native")
      // Отключаем S3 и Hive для локальной работы
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.hadoop.fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()

    try {
      // 1. Kafka → Raw (только если нет данных в Raw)
      if (shouldProcessKafka(spark)) {
        println("1. Starting Kafka to Raw processing...")
        val kafkaProcessor = new KafkaToRawProcessor(spark, KAFKA_BOOTSTRAP_SERVERS)
        kafkaProcessor.process(
          topic = KAFKA_TOPIC
        )
        println("Kafka processing initialized")
      } else {
        println("1. Skipping Kafka (raw data already exists)")
      }

      // 2. Raw → Mart processing
      println("2. Processing Raw to Mart...")
      val martProcessor = new RawToMartProcessor(spark)
      martProcessor.process(KAFKA_TOPIC)
      println("Mart processing completed")

      // 3. BI Integration
      println("3. Setting up BI connections...")
      val biConnector = new BIConnector(spark, RAW_PATH)
      biConnector.setupDataForBI()
      println("BI tables created")

      // 4. Log results
      logResults(spark)

      println("=== Pipeline completed successfully ===")

    } catch {
      case e: Exception =>
        println(s"Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
    }
  }

  private def shouldProcessKafka(spark: SparkSession): Boolean = {
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val rawPath = new org.apache.hadoop.fs.Path(RAW_PATH)

      // Если данные уже существуют, удаляем их для перезаписи
      if (fs.exists(rawPath)) {
        println(s"Raw data already exists at ${RAW_PATH}. Deleting for overwrite...")
        fs.delete(rawPath, true) // true для рекурсивного удаления
        println("Old raw data deleted")
      }

      // Всегда возвращаем true для обработки Kafka
      true

    } catch {
      case e: Exception =>
        println(s"Warning: Could not check/delete raw path: ${e.getMessage}")
        true // Все равно запускаем Kafka
    }
  }

  private def logResults(spark: SparkSession): Unit = {
    println("=== Processing Results ===")

    // Проверяем созданные витрины
    val martTables = Seq(
      ORDERS_BY_HOUR,
      REVENUE_BY_DISTRICT,
      KPI_METRICS
    )

    martTables.foreach { tableName =>
      val tablePath = s"output/$tableName"
      try {
        val df = spark.read.format("orc").load(tablePath)
        val count = df.count()
        println(s"$tableName: $count rows")
        if (count > 0) {
          println(s"Sample: ${df.take(1).head}")
        }
      } catch {
        case e: Exception =>
          println(s"$tableName: ERROR - ${e.getMessage}")
      }
    }

    // Примеры запросов для дашбордов
    println("=== BI Dashboard Queries ===")
    println("1. Заказы по часам:")
    println(s"   SELECT hour, order_count FROM ${ORDERS_BY_HOUR} ORDER BY hour")

    println("2. Выручка по районам:")
    println(s"   SELECT district, revenue, city FROM ${REVENUE_BY_DISTRICT} ORDER BY revenue DESC LIMIT 10")

    println("3. KPI метрики:")
    println(s"   SELECT metric_name, value FROM ${KPI_METRICS} WHERE date = CURRENT_DATE")
  }

}