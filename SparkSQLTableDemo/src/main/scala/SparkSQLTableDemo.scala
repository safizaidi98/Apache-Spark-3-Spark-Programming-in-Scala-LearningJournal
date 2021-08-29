import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLTableDemo extends Serializable {

  @transient lazy val logger : Unit = Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "dataSource/flight-time.parquet")
      .load()

    flightTimeParquetDF.show(5)

    spark.sql(
      """
        |Create database if not exists AIRLINE_DB
        |""".stripMargin)

    spark.sql(
      """
        | show databases
        |""".stripMargin).show()

//    logger.info(spark.catalog.currentDatabase)

    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .bucketBy(5, "ORIGIN", "OP_CARRIER")
        .sortBy("ORIGIN", "OP_CARRIER")
        .option("path", "spark-warehouse/")
        .saveAsTable("flight_data_tbl")

  spark.catalog.listTables("AIRLINE_DB").show()

    spark.stop()
  }
}
