import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SimpleAggDemo extends Serializable {
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder()
      .appName("Log File Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "FAILFAST")
      .option("path", "data/invoices.csv")
      .load()

    invoiceDF.show(5, truncate = false)

    //      SIMPLE AGGREGATION

    invoiceDF.select(
      count("*").as("Total Count"),
      sum("Quantity").as("SummedQuantity"),
    ).show()

    invoiceDF.selectExpr(
      "count(1) as totalCount",
      "count(StockCode) as TotalStock",
      "sum(Quantity) as SummedQuantity"
    ).show()


    //    GROUP AGGREGATION

    //    invoiceDF.select(
    //      count("*").as("Total Count"),
    //      sum("Quantity").as("SummedQuantity")
    //    ).groupBy("Country")

    invoiceDF.createOrReplaceTempView("invoiceDFView")

    spark.sql(
      """
        | select
        | Country,
        | InvoiceNo,
        | sum(Quantity * UnitPrice) as BillAmnt
        |
        | from invoiceDFView
        |
        | group by
        | Country,
        | InvoiceNo
        |
        |""".stripMargin).show()


    invoiceDF.printSchema()

    val exSummaryDF = invoiceDF.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.m"))
      .where("YEAR(InvoiceDate) = 2010")
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(
        countDistinct("InvoiceNo").as("NumInvoices"),
        sum("Quantity").as("SummedQuantity"),
        expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")
      )


    exSummaryDF
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("output/")

    exSummaryDF
      .sort("Country", "WeekNumber")
      .show()


    val summaryDF = spark.read
      .format("parquet")
      .option("path", "data/summary.parquet")
        .load()

//          NOW WE DEFFINE OUT WINDOW

    val runningTotalWindow = Window
      .partitionBy("Country")
        .orderBy("WeekNumber")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF.withColumn("RunningTotal",
     sum("InvoiceValue").over(runningTotalWindow)
    ).show()


    spark.stop()
  }
}
