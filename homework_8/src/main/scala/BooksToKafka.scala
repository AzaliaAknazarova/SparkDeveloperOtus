import model.BookModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BooksToKafka {
  def main(args: Array[String]): Unit = {
    // Инициация спарка
    val spark = SparkSession.builder()
      .appName("BooksToKafka")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val resourcePathCsvBook = getClass.getResource("/csv/bestsellers_with_categories.csv").getPath

    import spark.implicits._

    val books = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(resourcePathCsvBook)
      .as[BookModel]

    val booksJson = books.select(to_json(struct($"*")).alias("value"))

    booksJson.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "books")
      .option("checkpointLocation", "/tmp/kafka-books-checkpoint")
      .save()

  }
}