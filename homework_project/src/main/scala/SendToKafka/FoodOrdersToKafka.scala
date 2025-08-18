package SendToKafka

import model.{NormalizeOrderModel, RawOrderModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FoodOrdersToKafka {
  def main(args: Array[String]): Unit = {
    // Инициация спарка
    val spark = SparkSession.builder()
      .appName("FoodOrdersToKafka")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val resourcePathCsv = getClass.getResource("/csv/order_kaggle_data.csv").getPath

    import spark.implicits._

    // 1. Чтение CSV с переименованием в select
    val rawOrderDS = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("multiLine", "true") // Важно для полей с переносами
      .option("escape", "\"") // Экранирование кавычек
      .option("quote", "\"") // Указываем что поля в кавычках
      .csv(resourcePathCsv)
      .select(
        $"Restaurant ID".alias("restaurant_id"),
        $"Restaurant name".alias("restaurant_name"),
        $"Subzone".alias("district"),
        $"City".alias("city"),
        $"Order ID".alias("order_id"),
        $"Order Placed At".alias("order_time"),
        $"Order Status".alias("status"),
        $"Distance".alias("distance"),
        $"Items in order".alias("items"),
        $"Bill subtotal".alias("bill_subtotal"),
        $"Total".alias("total"),
        $"Rating".alias("rating"),
        $"KPT duration (minutes)".alias("prep_time"),
        $"Rider wait time (minutes)".alias("rider_wait_time"),
        $"Customer ID".alias("customer_id")
      )
      .as[RawOrderModel]

    // 2. Преобразование в нормализованный вид
    val normalizedDS = rawOrderDS.map(NormalizeOrderModel.fromRaw)

    // 3. Отправка в Kafka
    normalizedDS
      .select(to_json(struct($"*")).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "food_orders_pj")
      .option("checkpointLocation", "/tmp/kafka-foods-checkpoint")
      .save()

    // Для отладки
    println("Пример преобразованных данных:")
    normalizedDS.show(100, truncate = false)
  }
}