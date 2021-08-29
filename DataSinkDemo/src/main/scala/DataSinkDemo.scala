import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession, functions}


object DataSinkDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight-time.parquet")
      .load()

    logger.info("Number " + flightTimeParquetDF.rdd.getNumPartitions)

    println("Number of Partitition : " + flightTimeParquetDF.rdd.getNumPartitions)
    flightTimeParquetDF.groupBy(functions.spark_partition_id()).count().show()

    val partitionedDF = flightTimeParquetDF.repartition(5)

    println("Number of Partitition : " + partitionedDF.rdd.getNumPartitions)
    partitionedDF.groupBy(functions.spark_partition_id()).count().show()


    //    flightTimeParquetDF.write
//        .format("avro")
//        .mode(SaveMode.Overwrite)
//        .option("path", "dataSink/avro/")
//        .save()


    flightTimeParquetDF.write
            .format("json")
            .mode(SaveMode.Overwrite)
            .option("path", "dataSink/json/")
            .partitionBy("OP_CARRIER", "ORIGIN")
            .save()






  spark.stop()
  }
}
