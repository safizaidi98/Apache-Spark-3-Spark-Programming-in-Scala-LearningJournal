import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Join Demo")
      .master("local[3]")
      .getOrCreate()

    val ordersList = List(
      ("01", "02", 350, 1),
      ("01", "04", 580, 1),
      ("01", "07", 320, 2),
      ("02", "03", 450, 1),
      ("02", "06", 220, 1),
      ("03", "01", 195, 1),
      ("04", "09", 270, 3),
      ("04", "08", 410, 2),
      ("05", "02", 350, 1)
    )

    val productList = List(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 350, 20),
      ("03", "Wireless Mouse", 450, 50),
      ("04", "Wireless Keyboard", 580, 50),
      ("05", "Standard Keyboard", 360, 10),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )

    val orderDF = spark.createDataFrame(ordersList).toDF("order_id", "prod_id", "unit_price", "qty")
    val productDF = spark.createDataFrame(productList).toDF("prod_id", "prod_name", "list_price", "qty")

    orderDF.show()
    productDF.show()


    val productQtyRenamed = productDF.withColumnRenamed("qty", "reorder_qty")
    val joinExpr = orderDF.col("prod_id") === productQtyRenamed.col("prod_id")

    orderDF.join(productQtyRenamed, joinExpr, "Inner" )
      .select("order_id", "prod_name", "unit_price", "qty", "reorder_qty")
      .show(20)

    productQtyRenamed.show()


    orderDF.join(productQtyRenamed, joinExpr, "left" )
      .drop(productQtyRenamed.col("prod_id"))
      .select("order_id", "prod_id", "prod_name", "unit_price", "qty", "reorder_qty")
      .withColumn("prod_name", expr("coalesce(prod_name, prod_id)"))
      .show(20)


    spark.stop()
  }
}
