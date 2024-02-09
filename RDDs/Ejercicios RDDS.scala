// Databricks notebook source
// MAGIC %md
// MAGIC Ejercicios RDDs

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Lee todos los archivos de texto de un directorio en un único RDD.
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._

// COMMAND ----------

// File location and type
val fileLocation = "/FileStore/tables/DataSetPartidos.txt"
val fileType = "text"

val df = spark.read.format(fileType).load(fileLocation)

// Muestra el DataFrame
display(df)


// COMMAND ----------

// MAGIC %md
// MAGIC 2. Lee varios archivos de texto en un solo RDD

// COMMAND ----------

val spark = SparkSession.builder.appName("LecturaVariosArchivos").getOrCreate()

val archivos = Array("/FileStore/tables/accounts.csv", "/FileStore/tables/DataSetPartidos.txt")

val rdd = spark.sparkContext.textFile(archivos.mkString(","))

val schema = new StructType().add("contenido", StringType, true)

val rddRows = rdd.map(Row(_))
val dataframe = spark.createDataFrame(rddRows, schema)

display(dataframe)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Lee todos los archivos de texto que coinciden con un patrón.
// MAGIC

// COMMAND ----------

val spark = SparkSession.builder.appName("LecturaVariosArchivos").getOrCreate()

val patron = "/FileStore/tables/*.txt"

val dataframe = spark.read.text(patron)

display(dataframe)


// COMMAND ----------

// MAGIC %md
// MAGIC 4. Lee archivos de varios directorios en un solo RDD.
// MAGIC

// COMMAND ----------

/*No se exactamente como meter los archivos en diferentes directorios, pero supongo que para leerlos es igual que el ejercicio 2*/

// COMMAND ----------

// MAGIC %md
// MAGIC 5. Lectura de archivos de texto de directorios anidados en un único RDD.
// MAGIC

// COMMAND ----------

val spark = SparkSession.builder.appName("LecturaVariosArchivos").getOrCreate()

val directorioPrincipal = "/FileStore/tables/pruebaRDD"

val rdd = sc.wholeTextFiles(s"$directorioPrincipal/*").values

val schema = new StructType().add("contenido", StringType, true)

val rddRows = rdd.map(Row(_))
val dataframe = spark.createDataFrame(rddRows, schema)
val dataframeSplit = dataframe.withColumn("contenido", split(col("contenido"), ";"))

display(dataframe)


// COMMAND ----------

// MAGIC %md
// MAGIC 6. Lectura de varios archivos csv en RDD.
// MAGIC

// COMMAND ----------

val spark = SparkSession.builder.appName("LecturaVariosArchivosCSV").getOrCreate()

val patron = "/FileStore/tables/*.csv"

val dataframe = spark.read.csv(patron)

display(dataframe)
