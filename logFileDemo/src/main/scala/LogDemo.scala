import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LogDemo extends Serializable {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Log File Demo")
        .master("local[3]")
        .getOrCreate()

    val logDF = spark.read.textFile("data/apache_logs.txt")




      spark.stop()
    }
  }
