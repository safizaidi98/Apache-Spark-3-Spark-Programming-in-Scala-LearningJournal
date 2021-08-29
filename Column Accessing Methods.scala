// Databricks notebook source
// MAGIC %fs ls /databricks-datasets

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/airlines

// COMMAND ----------

// MAGIC %fs head dbfs:/databricks-datasets/airlines/part-00000

// COMMAND ----------

val airlinesDF = spark.read
    .format("csv")
    .option("header", "true")
.option("inferSchema", "true")
.option("sampleRatio", "0.001")
.load("dbfs:/databricks-datasets/airlines/part-00000")

// COMMAND ----------

airlinesDF.show(5)

// COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance", "Year", "Month", "DayofMonth").show(5)

// COMMAND ----------

airlinesDF.selectExpr("Origin", "Dest", "Distance", "to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as DateMy" ).show(5)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


