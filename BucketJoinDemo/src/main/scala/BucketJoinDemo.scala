import org.apache.spark.sql.{SaveMode, SparkSession}

object BucketJoinDemo extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bucket Join Demo")
      .master("local[3]")
      .getOrCreate()

    val flightTimeDF1 = spark.read.json("data/d1/")
    val flightTimeDF2 = spark.read.json("data/d2/")

    flightTimeDF1.show()
    flightTimeDF2.show()

    spark.sql(
      """
        |Create database if not exists MY_DB
        |""".stripMargin)

    spark.sql(
      """
        | use MY_DB
        |""".stripMargin)


    flightTimeDF1.coalesce(1).write
        .bucketBy(3, "id")
        .mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/flight_data1/")
        .saveAsTable("flight_data1")


    flightTimeDF2.coalesce(1).write
      .bucketBy(3, "id")
      .mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/flight_data2/")
      .saveAsTable("flight_data2")

    spark.catalog.listDatabases().show()


    val df3 = spark.read.table("MY_DB.flight_data1")
    val df4 = spark.read.table("MY_DB.flight_data2")

    val joinExpr = df3.col("id") === df4.col("id")

//                Setting the below prop to stop shuffle of BroadCast Join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val joinDF = df3.join(df4, joinExpr, "inner")

    joinDF.foreach(_ => ())
    scala.io.StdIn.readLine()



    spark.stop()
  }
}
