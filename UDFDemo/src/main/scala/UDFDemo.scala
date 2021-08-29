import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UDFDemo extends Serializable {
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder()
      .appName("Log File Demo")
      .master("local[3]")
      .getOrCreate()

    val surveyDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/survey.csv")
      .option("inferSchema", "true")
      .load()

    surveyDF.show(5)
    surveyDF.select("Gender").distinct().show(truncate = false)

//        Registering our defined function to Spark UDF at Driver for Column Object Expr

    import org.apache.spark.sql.functions._

    val parseGenderUDF = udf(parseGender(_ : String))
    surveyDF.withColumn("Gender", parseGenderUDF(col("Gender"))).show(20)


    //        Registering our defined function to Spark UDF at Driver for String / SQL Expr

    spark.udf.register("parseGenderSQL", parseGender(_ : String))

    spark.catalog.listFunctions().filter(r => r.name == "parseGenderSQL").show(truncate = false)

    surveyDF.withColumn("Gender", expr("parseGenderSQL(Gender)")).show(20)


    spark.stop()
  }

  def parseGender(s : String) = {
    val maleString = "^m$|ma|m.l".r
    val femaleString = "^f$|f.m|w.m".r

    if(femaleString.findFirstIn(s.toLowerCase).nonEmpty)"Female"
    else if (maleString.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"

  }

}
