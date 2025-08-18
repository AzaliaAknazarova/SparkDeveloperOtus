package FromKafka

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

class KafkaToRawProcessor(spark: SparkSession, bootstrapServers: String) {
  def process(topic: String): Unit = {
    // Чтение из Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Запись в Raw Layer (Parquet)
    kafkaStream
      .select(col("value").cast("string"))
      .writeStream
      .format("parquet")
      .option("path", s"output/raw_$topic")
      .option("checkpointLocation", s"output/checkpoints/$topic")
      .outputMode("append")
      .start()
      .awaitTermination(30000)
  }
}