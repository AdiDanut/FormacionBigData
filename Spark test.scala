// Databricks notebook source
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// COMMAND ----------

val spark = SparkSession.builder
    .appName("ChargePointsETLJob")
    .getOrCreate()

def extract(sparkSession: SparkSession, path: String): DataFrame = {
  val df = sparkSession.read.option("header", "true").csv(path)
  df
}

def transform(dataframe: DataFrame): DataFrame = {
  val resultDataFrame = dataframe
  .groupBy(col("CPID") as "chargepoint_id")
  .agg(
      avg("PluginDuration") as "avg_duration",
      max("PluginDuration") as "max_duration"
    )
  resultDataFrame
}

def load(dataframe: DataFrame): Unit = {
  dataFrame.write
    .mode("overwrite")
    .parquet("/FileStore/tables/output_path")
}

val dataFrame = extract(spark, "dbfs:/FileStore/tables/electric_chargepoints_2017.csv")
val dataFrameTransformed = transform(dataFrame)

load(dataFrameTransformed)



