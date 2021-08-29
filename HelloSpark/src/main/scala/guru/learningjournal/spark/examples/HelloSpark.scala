package guru.learningjournal.spark.examples

import java.util.Properties
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

//    val sparkAppConf = new SparkConf()

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val df = loadSurveyDF(spark, "data/sample.csv")
    df.repartition(2)
    val countDf = countByCountry(df)

    countDf.foreach(r => logger.info(r(0) + " : " + r(1)))

    logger.info(countDf.collect().mkString("->"))

    println(countDf.rdd.getNumPartitions)

//    scala.io.StdIn.readLine()
//    df.show(5)
//    df.printSchema()

    spark.stop()
  }

  def countByCountry(df : DataFrame) ={
    df.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, file : String) = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(file)
  }

  def getSparkAppConf : SparkConf = {
    val sparkAppConf = new SparkConf()
    val prop = new Properties()

    prop.load(Source.fromFile("spark.conf").bufferedReader())
    prop.forEach( (k, v) => sparkAppConf.set(k.toString, v.toString))

    sparkAppConf
  }

}
