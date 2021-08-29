import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object RowDemo extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EventDate", StringType)
    ))

    val myRows = List(
      Row("123", "04/05/2020"),
      Row("124", "4/5/2020"),
      Row("125", "06/07/2020")
    )

    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    val myDF = spark.createDataFrame(myRDD, mySchema)

    myDF.printSchema()
    myDF.show(5)

    val newDF = convertDateDF(myDF, "EventDate", "M/d/y")

    newDF.printSchema()
    newDF.show(5)




    spark.stop()
  }

  def convertDateDF(myDF : DataFrame, colName : String, frmt : String) = {
    myDF.withColumn(colName, to_date(col(colName), frmt))
  }

}
