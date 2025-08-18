package BI

import org.apache.spark.sql.SparkSession

class BIConnector(spark: SparkSession, martPath: String) {

  def setupDataForBI(): Unit = {
    println("=== BI Data Preparation ===")

    // 1. Создаем CSV экспорт для удобства
    createCSVExports()

    // 2. Показываем пути для прямого доступа к ORC
    printDirectAccessPaths()

    // 3. Показываем примеры запросов
    printExampleQueries()
  }

  private def createCSVExports(): Unit = {
    val tables = List("orders_by_hour", "revenue_by_district", "kpi_metrics")

    tables.foreach { tableName =>
      try {
        val df = spark.read.format("orc").load(s"$martPath/$tableName")
        val csvPath = s"$martPath/export/$tableName"

        df.coalesce(1)
          .write
          .option("header", "true")
          .mode("overwrite")
          .csv(csvPath)

        println(s"CSV export: $csvPath")
      } catch {
        case e: Exception =>
          println(s"CSV export failed for $tableName: ${e.getMessage}")
      }
    }
  }

  private def printDirectAccessPaths(): Unit = {
    println("\n=== Direct ORC Access Paths ===")
    println("For BI tools use direct ORC paths:")
    println()
    println("1. Заказы по часам:")
    println(s"   SELECT * FROM orc.`$martPath/orders_by_hour`")
    println()
    println("2. Выручка по районам:")
    println(s"   SELECT * FROM orc.`$martPath/revenue_by_district`")
    println()
    println("3. KPI метрики:")
    println(s"   SELECT * FROM orc.`$martPath/kpi_metrics`")
  }

  private def printExampleQueries(): Unit = {
    println("\n=== Example Queries ===")
    println("1. Заказы по часам:")
    println("   SELECT hour, order_count FROM orders_by_hour ORDER BY hour")
    println()
    println("2. Топ районов по выручке:")
    println("   SELECT district, revenue FROM revenue_by_district ORDER BY revenue DESC LIMIT 10")
    println()
    println("3. KPI метрики:")
    println("   SELECT metric_name, value FROM kpi_metrics WHERE date = CURRENT_DATE")
  }
}