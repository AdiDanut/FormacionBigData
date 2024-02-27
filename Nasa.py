# Databricks notebook source
# MAGIC %md
# MAGIC <h3>Ejercicios Nasa</h3>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Creamos nuestro DataFrame</h5>

# COMMAND ----------

spark = SparkSession.builder.appName("log_analysis").getOrCreate()

schema = ["host", "ident", "user", "timestamp", "method", "resource", "protocol", "status", "size"]

raw_logs_rdd = spark.sparkContext.textFile("/FileStore/tables/access_log_Aug95")

log_pattern = r'(\S+) - - \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\S+) (\S+)'

structured_logs_rdd = raw_logs_rdd.flatMap(lambda line: re.findall(log_pattern, line + " placeholder"))

log_schema = ["host", "date_str", "method", "resource", "protocol", "status_code", "size"]

logs_df = spark.createDataFrame(structured_logs_rdd, log_schema)

logs_df = (
    logs_df
    .withColumn("date_str", regexp_replace("date_str", " -0400", ""))
    .withColumn("date", to_timestamp("date_str", "dd/MMM/yyyy:HH:mm:ss"))
    .drop("date_str")
)

display(logs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Guardamos el DataFrame en formato parquet</h5>

# COMMAND ----------

logs_df.write.parquet("/FileStore/tables/log_data_parquet3", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.</h5>

# COMMAND ----------

data_frame_parquet = spark.read.parquet("/FileStore/tables/log_data_parquet3")

protocol_counts = data_frame_parquet \
    .groupBy("protocol") \
    .agg(count("protocol").alias("count")) \
    .orderBy("count", ascending=False)
display(protocol_counts)



# COMMAND ----------

# MAGIC %md
# MAGIC <h5>- ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos 
# MAGIC para ver cuál es el más común</h5>

# COMMAND ----------

status_code_counts = data_frame_parquet \
    .groupBy("status_code") \
    .agg(count("status_code").alias("count")) \
    .orderBy("count", ascending=False)
display(status_code_counts)



# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Y los métodos de petición (verbos) más utilizados?</h5>

# COMMAND ----------

method_counts = data_frame_parquet \
    .groupBy("method") \
    .agg(count("method").alias("count")) \
    .orderBy("count", ascending=False)
display(method_counts)

# COMMAND ----------

resource_bytes = logs_df.groupBy("resource"). \
    agg(sum("size").alias("total_bytes")). \
    orderBy("total_bytes", ascending=False)

display(resource_bytes)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es 
# MAGIC decir, el recurso con más registros en nuestro log.</h5>

# COMMAND ----------

resource_counts = data_frame_parquet \
    .groupBy("resource") \
    .agg(count("resource").alias("count")) \
    .orderBy("count", ascending=False)
display(resource_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Qué días la web recibió más tráfico?</h5>

# COMMAND ----------

logs_df = logs_df.withColumn("date", to_date("date", "dd/MMM/yyyy"))

daily_traffic = logs_df.groupBy("date"). \
    agg(sum("size").alias("total_bytes")). \
    orderBy(desc("total_bytes"))

display(daily_traffic)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Cuáles son los hosts son los más frecuentes?</h5>

# COMMAND ----------

host_counts = data_frame_parquet \
    .groupBy("host") \
    .agg(count("host").alias("count")) \
    .orderBy("count", ascending=False)
display(host_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> ¿A qué horas se produce el mayor número de tráfico en la web?</h5>

# COMMAND ----------

hourly_traffic_column = data_frame_parquet.withColumn("hour", hour("date"))

hourly_traffic = hourly_traffic_column.groupBy("hour"). \
    agg(sum("size").alias("total_bytes")). \
    orderBy(desc("total_bytes"))
display(hourly_traffic)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Cuál es el número de errores 404 que ha habido cada día?</h5>

# COMMAND ----------

error_404_df = data_frame_parquet. \
    withColumn("date", to_date("date", "dd/MMM/yyyy")). \
    filter(data_frame_parquet["status_code"] == 404). \
    groupBy("date"). \
    agg(count("*").alias("404_count")). \
    orderBy(desc("404_count"))

display(error_404_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>¿Qué generación es más poderosa?</h5>

# COMMAND ----------

val powerByGenerationDF = pokemonDF.groupBy("Generation")
  .agg(sum("Total").alias("TotalPower"))
  .sort("Generation")

  display(popowerByGenerationDFwerG)
