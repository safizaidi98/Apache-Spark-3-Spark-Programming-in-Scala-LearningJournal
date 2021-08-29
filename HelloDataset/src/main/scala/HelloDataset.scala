import org.apache.spark.sql.SparkSession

case class  SurveryClass(Age : Int, Gender:String, Country:String,State : String)

object HelloDataset extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Hello Spark").master("local[2]").getOrCreate()


    val rawDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/sample.csv")

    rawDF.show(3)

    import spark.implicits._

    val surveyDS = rawDF.select("Age", "Gender", "Country", "state").as[SurveryClass]

    surveyDS.filter(a => a.Age < 40)
//    surveyDS.filter("a.Age < 40")

    rawDF.createOrReplaceTempView("rawDFView")

    val countCountryDF = spark.sql(
      """
        |select Country, count(1) from rawDFView where Age < 40 group by Country
        |""".stripMargin)

    countCountryDF.show()

    spark.stop()
    //    val ds = rawDF.toDS
  }
}
