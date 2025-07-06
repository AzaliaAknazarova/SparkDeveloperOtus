import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HW - 9")
      .master("local[*]")
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val resourcePathCsvIris = getClass.getResource("/csv/IRIS.csv").getPath

    import spark.implicits._

    val irisData = spark.read
      .option("delimiter",",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(resourcePathCsvIris)

    // Определяем признаки
    val numericColumnsFinal = Array("sepal_length", "sepal_width", "petal_length", "petal_width")
    //val categoricalColumns = Array("species")

    // Индексация категориального признака (label)
    val labelIndexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("label")

    // Векторизация числовых признаков (assembler)
    val assembler = new VectorAssembler()
      .setInputCols(numericColumnsFinal)
      .setOutputCol("numericFeatures")

    // Нормализация числовых признаков
    val scaler = new MinMaxScaler()
      .setInputCol("numericFeatures")
      .setOutputCol("scaledFeatures")

    val selector = new UnivariateFeatureSelector()
      .setFeatureType("continuous") // тип признаков - числовые
      .setLabelType("categorical")  // тип целевой переменной
      .setSelectionMode("numTopFeatures") // режим отбора - кол-во лучших признаков
      .setSelectionThreshold(4) // всего 4 признака
      .setFeaturesCol("scaledFeatures") // имя колонки содержащей вектор признаков
      .setLabelCol("label") // имя колонки содержащей целевую переменную (метка класса)
      .setOutputCol("selectedFeatures") // имя новой колонки -  вектор отобранных признаков


    // Модель (Random Forest)
    val rf = new RandomForestClassifier()
      .setFeaturesCol("selectedFeatures")
      .setLabelCol("label")
      .setNumTrees(20)

    // Собираем всё в Pipeline
    val pipeline = new Pipeline().setStages(Array(
      labelIndexer,
      assembler,
      scaler,
      selector,
      rf
    ))

    // Разбиваем на train/test
    val Array(training, test) = irisData.randomSplit(Array(0.8, 0.2), seed = 1234)

    // Обучаем модель
    val model = pipeline.fit(training)

    // Предсказания и оценка
    val predictions = model.transform(test)
    predictions.select("selectedFeatures", "label", "prediction").show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy = $accuracy")

    // Сохраняем модель
    model.write.overwrite().save("output/iris_full_pipeline_model")

  }
}