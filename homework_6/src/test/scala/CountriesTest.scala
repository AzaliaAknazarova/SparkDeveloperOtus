import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import Main._
import org.apache.spark.sql.types._

class CountriesTest extends AnyFunSpec with DataFrameComparer {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Countries Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val testJsonPath = getClass.getResource("/json/countries.json").getPath
  val df = spark.read.option("multiline", "true").json(testJsonPath)

  describe("countriesWithManyBorders") {

    it("should return expected schema") {
      val result = countriesWithManyBorders(df)
      assert(result.columns.contains("Country"))
      assert(result.columns.contains("NumBorders"))
      assert(result.columns.contains("BorderCountries"))
    }

    it("should contain known country with many borders (e.g. Germany)") {
      val result = countriesWithManyBorders(df)
      val countries = result.select("Country").as[String].collect()
      assert(countries.contains("Germany"))
    }
  }

  describe("manySpokenLanguages") {
    it("should correctly group countries by languages") {
      val result = manySpokenLanguages(df)
      val row = result.filter($"Language" === "English").first()
      assert(row.getAs[Long]("NumCountries") >= 1)
    }

    it("English, Russian и Spanish") {
      val resultDf = Main.manySpokenLanguages(df)
        .filter($"Language".isin("English", "Russian", "Spanish"))

      val expectedSchema = StructType(Seq(
        StructField("Language", StringType, nullable = true),
        StructField("NumCountries", LongType, nullable = false),
        StructField("Countries", ArrayType(StringType, containsNull = false), nullable = false)
      ))

      // Создаем эталонные данные для сравнения
      val expectedData = Seq(
        Row("English", 91L, Seq(
          "Christmas Island", "Sierra Leone", "Nigeria", "Norfolk Island", "Malta", "United Kingdom",
          "Solomon Islands", "Cameroon", "Samoa", "British Indian Ocean Territory", "Canada", "Rwanda",
          "Namibia", "Guernsey", "Bermuda", "Kenya", "Belize", "Philippines", "Jamaica", "Gibraltar",
          "Curaçao", "Guam", "New Zealand", "Uganda", "Vanuatu", "Puerto Rico", "Ireland", "Tuvalu",
          "Marshall Islands", "South Africa", "Sint Maarten", "Anguilla", "Nauru", "Malawi", "Palau",
          "American Samoa", "United States", "Heard Island and McDonald Islands", "Caribbean Netherlands",
          "Dominica", "Cook Islands", "Cocos (Keeling) Islands", "Saint Helena, Ascension and Tristan da Cunha",
          "Seychelles", "Gambia", "Zimbabwe", "Grenada", "Australia", "Niue", "Micronesia", "Liberia",
          "India", "British Virgin Islands", "South Georgia", "South Sudan", "Antigua and Barbuda", "Hong Kong",
          "Malaysia", "Zambia", "Eritrea", "Pakistan", "Northern Mariana Islands", "Cayman Islands", "Barbados",
          "Tonga", "Saint Kitts and Nevis", "Guyana", "Jersey", "Sudan", "Pitcairn Islands",
          "United States Minor Outlying Islands", "Falkland Islands", "Mauritius", "Singapore",
          "Saint Vincent and the Grenadines", "Eswatini", "Isle of Man", "Fiji", "Ghana", "Tanzania",
          "Turks and Caicos Islands", "Lesotho", "United States Virgin Islands", "Bahamas",
          "Saint Lucia", "Trinidad and Tobago", "Papua New Guinea", "Kiribati", "Botswana", "Montserrat", "Tokelau")),
        Row("Russian", 8L, Seq(
          "Turkmenistan", "Tajikistan", "Kazakhstan", "Azerbaijan", "Uzbekistan", "Russia", "Kyrgyzstan", "Belarus")),
        Row("Spanish", 24L, Seq(
          "Venezuela", "Bolivia", "Uruguay", "Spain", "Honduras", "Cuba", "Ecuador", "Belize", "Nicaragua",
          "Peru", "Guam", "Panama", "Colombia", "Mexico", "Puerto Rico", "El Salvador", "Guatemala",
          "Costa Rica", "Paraguay", "Argentina", "Chile", "Dominican Republic", "Equatorial Guinea",
          "Western Sahara"))
      )

      val sortedExpected = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)
        .orderBy("Language")

      // Отсортируем по Language, для надежного сравнения
      val sortedResult = resultDf.orderBy("Language")

      assertSmallDatasetEquality(
        sortedResult.select("Language", "NumCountries", "Countries"),
        sortedExpected.select("Language", "NumCountries", "Countries")
      )

    }
  }
}
