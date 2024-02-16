// Databricks notebook source
// MAGIC %md
// MAGIC Ejercicios DataFrames Avanzados
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// COMMAND ----------

val spark = SparkSession.builder.appName("GooglePlayStore").getOrCreate()

val fileLocationApps = "/FileStore/tables/googleplaystore_1.csv"
val dfApps = spark.read.option("header", "true").csv(fileLocationApps)

val fileLocationReviews = "/FileStore/tables/googleplaystore_user_reviews_1.csv"
val dfReviews = spark.read.option("header", "true").csv(fileLocationReviews)

display(dfApps)


// COMMAND ----------

// MAGIC %md
// MAGIC 1. Elimina la App “Life Made WI-FI Touchscreen Photo Frame”

// COMMAND ----------

val appToRemove = "Life Made WI-FI Touchscreen Photo Frame"
val dfAppsFiltered = dfApps.filter(col("App") =!= appToRemove)
display(dfAppsFiltered)

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Sustituir los valores NaN en la columna Rating.
// MAGIC

// COMMAND ----------

val dfAppsRating = dfApps.withColumn("Rating", when(isnan(col("Rating")), 0).otherwise(col("Rating")))
display(dfAppsRating)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Sustituir NaN en la columna Type por “Unknown”.

// COMMAND ----------

val dfAppsFixedType = dfApps.withColumn("Type", when(isnan(col("Type")), "Unknown").otherwise(col("Type")))
display(dfAppsFixedType)

// COMMAND ----------

// MAGIC %md
// MAGIC 4. Agregar una columna que nos indica si las características varían dependiendo del dispositivo

// COMMAND ----------

val dfAppsWithDeviceVariation = dfApps.withColumn("Features_Vary_by_Device",
  when(col("Size") === "Varies with device" || col("Current Ver") === "Varies with device", "Varia").otherwise("No varia")
)
display(dfAppsWithDeviceVariation)

// COMMAND ----------

// MAGIC %md
// MAGIC 5. Crea una nueva columna llamada Frec_Download con los siguientes valores.<br>
// MAGIC a. Baja => número de instalaciones < 50000 <br>
// MAGIC b. Media => número de instalaciones >= 50000 y < 1000000 <br>
// MAGIC c. Alta => número de instalaciones >= 1000000 y < 50000000 <br>
// MAGIC d. Muy alta => número de instalaciones >= 50000000 <br>

// COMMAND ----------

val installs = col("Installs")

val dfWithFrecDownload = dfAppsWithDeviceVariation
  .withColumn("Installs", translate(installs, "+,", "").cast("int"))
  .withColumn("Frec_Download",
    when(installs.isNull, "Unknown")
      .when(installs < 50000, "Baja")
      .when(installs >= 50000 && installs < 1000000, "Media")
      .when(installs >= 1000000 && installs < 50000000, "Alta")
      .when(installs >= 50000000, "Muy alta")
  )

display(dfWithFrecDownload)

// COMMAND ----------

// MAGIC %md
// MAGIC Consultas sobre df_limpio <br>
// MAGIC a. Muestra aquellas aplicaciones que tengan una frecuencia de descarga muy alta y una valoración mayor a 4.5<br>
// MAGIC b. Muestra el número de aplicaciones con frecuencia de descarga muy alta y de coste gratuito.<br>
// MAGIC c. Muestra aquellas aplicaciones cuyo precio sea menor que 13 dólares<br>

// COMMAND ----------

val queryA = dfWithFrecDownload.filter("Frec_Download = 'Muy alta' AND Rating > 4.5")
display(queryA)

// COMMAND ----------

val queryB = dfWithFrecDownload.filter("Frec_Download = 'Muy alta' AND Type = 'Free'")
println(queryB.count())

// COMMAND ----------

val queryC = dfWithFrecDownload.filter("Price < 13")
display(queryC)

// COMMAND ----------

// MAGIC %md
// MAGIC 7. Dado que nuestro set de datos contiene muchos registros, para comprobar nuestros resultados puedes hacer pruebas en una porción del dataset. Prueba usando el 10% de nuestros datos y con seed = 123.
// MAGIC

// COMMAND ----------

val seed = 123
val sampleQuery = dfWithFrecDownload.sample(withReplacement = false, fraction = 0.1, seed = seed)
display(sampleQuery)
