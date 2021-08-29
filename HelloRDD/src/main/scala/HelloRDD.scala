import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object HelloRDD extends Serializable {

  @transient val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val sparkAppConf = new SparkConf()
    sparkAppConf.setAppName("Hello Spark")
    sparkAppConf.setMaster("local[3]")

    val sparkContext = new SparkContext(sparkAppConf)

    val linesRDD = sparkContext.textFile("data/sample.csv")

    val cols = linesRDD.map(_.split(","))

//    logger.info(cols.collect().mkString(","))
    cols.foreach(println)
    sparkContext.stop()
//    val dataRDD =
  }
}
