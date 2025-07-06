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

    // Чтение данных из файла
    val jsonDataDf = spark.read
      .option("multiline", "true")
      .json(getClass.getResource("/json/countries.json").getPath)

    jsonDataDf.show(false)

    countriesWithManyBorders(jsonDataDf).show(false)
    manySpokenLanguages(jsonDataDf).show(false)

    spark.stop()
  }

  def countriesWithManyBorders(df: DataFrame): DataFrame = {
    val result = df
      .filter(size(col("borders")) >= 5)
      .withColumn("NumBorders", size(col("borders")))
      .withColumn("BorderCountries", concat_ws(",", col("borders")))
      .select(
        col("name.common").as("Country"),
        col("NumBorders"),
        col("BorderCountries")
      )
      .orderBy("Country")

    result.write.mode("overwrite").parquet("output/countries_with_many_borders")
    result
  }

  def manySpokenLanguages(df: DataFrame): DataFrame = {
    val spark = SparkSession.active
    import spark.implicits._

    // Получаем список всех полей в struct languages из схемы
    val languageFields = df.schema("languages").dataType
      .asInstanceOf[org.apache.spark.sql.types.StructType]
      .fields.map(_.name)

    // Создаем массив из всех языков (полей languages)
    val languagesArrayCol = array(languageFields.map(f => col(s"languages.$f")): _*)

    // explode массива языков
    val exploded = df
      .select(
        col("name.common").as("Country"),
        explode(languagesArrayCol).as("Language")
      )
      .filter($"Language".isNotNull && length(trim($"Language")) > 0)

    // Группируем по языку и считаем количество стран
    val result = exploded
      .groupBy("Language")
      .agg(
        countDistinct("Country").as("NumCountries"),
        collect_set("Country").as("Countries")
      )
      .orderBy(desc("NumCountries"))

    // Сохраняем результат
    result.write.mode("overwrite").parquet("output/language_ranking")
    result
  }
}