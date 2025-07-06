import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaBooksConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BooksFromKafkaConsumer")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    import spark.implicits._

    val schema = new StructType()
      .add("Name", StringType)
      .add("Author", StringType)
      .add("UserRating", DoubleType)
      .add("Reviews", IntegerType)
      .add("Price", IntegerType)
      .add("Year", IntegerType)
      .add("Genre", StringType)

    val kafkaSource = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "books")
      .option("startingOffsets", "earliest")
      .load()

    val books = kafkaSource
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .filter($"UserRating" >= 4.0)

    books.writeStream
      .format("parquet")
      .option("path", "output/books_filtered")
      .option("checkpointLocation", "output/checkpoints/books_filtered")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}