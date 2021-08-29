import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._

object SparkSchemaDemo extends Serializable
{
  @transient val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

//    Deffining Schema

    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))



    val flightTimeCsvDF = spark.read
        .format("csv")
        .option("header", "true")
        .option("path", "data/flight*.csv")
        .option("mode", "FAILFAST")
        .option("dateFormat", "d/M/y")
        .schema(flightSchemaStruct)
        .load()

    flightTimeCsvDF.show(5)
    logger.info(flightTimeCsvDF.schema.simpleString)

    //    println(flightTimeCsvDF.schema.simpleString)


//    val flightTimeJsonDF = spark.read
//      .format("json")
//      .option("path", "data/flight*.json")
//      .load()
//
//    flightTimeJsonDF.show(5)
////    println(flightTimeJsonDF.schema.simpleString)
//    logger.info(flightTimeJsonDF.schema.simpleString)


//    SIMILAR PROCESS FOR PARQUET FILE




    spark.stop()
  }
}
