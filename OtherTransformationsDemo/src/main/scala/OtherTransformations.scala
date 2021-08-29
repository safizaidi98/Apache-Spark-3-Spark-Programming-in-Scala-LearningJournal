import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object OtherTransformations extends Serializable {
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder()
      .appName("Log File Demo")
      .master("local[3]")
      .getOrCreate()

    val dataList = List(
      ("Ravi", "28", "1", "2002"),
      ("Abdul", "23", "5", "81"), // 1981
      ("John", "12", "12", "6"), // 2006
      ("Rosy", "7", "8", "63"), // 1963
      ("Abdul", "23", "5", "81")
    )

    val df = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(3)

//        We can convert to defferent type in 2 ways


//                      1 - Way   using Cast every case
    df.withColumn("Unique_id", monotonically_increasing_id())
      .withColumn("year", expr(
        """
          |case
          |when year < 20 then cast((year + 2000) as int)
          |when year < 100 then cast ((year + 1900) as int)
          |else year
          |end
          |""".stripMargin)).show()


    //                      1 - Way   using Cast the expr()
    df.withColumn("Unique_id", monotonically_increasing_id())
      .withColumn("year", expr(
        """
          |case
          |when year < 20 then cast((year + 2000) as int)
          |when year < 100 then cast ((year + 1900) as int)
          |else year
          |end
          |""".stripMargin).cast(IntegerType)).show()

//      .withColumn("year",
//        when(col("year") < 21, col("year") + 2000)
//          when(col("year") < 100, col("year") + 1900)
//          otherwise (col("year"))




      // .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
//      .withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"),"d/M/y"))
//      .drop("day", "month", "year")
//      .dropDuplicates("name", "dob")
//      .sort(expr("dob desc"))
//    finalDF.show






    spark.stop()
  }
}
