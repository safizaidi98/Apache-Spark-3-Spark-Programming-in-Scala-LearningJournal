import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShuffleJoinDemo extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    val flightTimeDF1 = spark.read
      .format("json")
      .load("data/d1/")


    val flightTimeDF2 = spark.read
      .format("json")
      .load("data/d2/")

    println("Partitions : " + flightTimeDF1.rdd.getNumPartitions)
    println("Partitions : " + flightTimeDF2.rdd.getNumPartitions)

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    val joinExpr = flightTimeDF1.col("id") === flightTimeDF2.col("id")
    val joinDF = flightTimeDF1.join(flightTimeDF2, joinExpr, "inner")

    spark.stop()
  }
}
