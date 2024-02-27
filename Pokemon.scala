// Databricks notebook source
// MAGIC %md
// MAGIC <h3>EJERCICIO DATASET POKEMON</h3>

// COMMAND ----------

// MAGIC %md
// MAGIC Crear una tabla llamada pokemonStats y carga los datos del csv pokemon.csv.
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val spark = SparkSession.builder
  .appName("PokemonStats")
  .getOrCreate()

val csvPath = "/FileStore/tables/Pokemon_1.csv"  

var pokemonDF = spark.read
  .option("header", "true")
  .option("delimiter", ",")
  .csv(csvPath)

val newColumnNames = Seq(
 "id", "Name", "Type1", "Type2", "Total", "HP", "Attack", "Defense", "SpAtk", "SpDef", "Speed", "Generation", "Legendary"
)
pokemonDF = pokemonDF.toDF(newColumnNames: _*)

pokemonDF.write
  .option("header", "true")
  .saveAsTable("pokemonStats")
  
display(pokemonDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>Borramos la tabla para crear una nueva con regex</h5>

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS pokemonStats")

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>Crear la misma tabla con expresiones regulares</h5>

// COMMAND ----------

/* Al estar delimitado solamente por ',' no veo el sentido para utilizar regex, ya que las columnas de los datos estan limpias tambien, por lo que el regex seria comprobar simplemente si hay una ',' */

val regexDelimiter = ","

var pokemonDF_regex = spark.read
  .option("header", "true")
  .option("delimiter", regexDelimiter)
  .csv(csvPath)

val newColumnNames = Seq(
 "id", "Name", "Type1", "Type2", "Total", "HP", "Attack", "Defense", "SpAtk", "SpDef", "Speed", "Generation", "Legendary"
)
pokemonDF_regex = pokemonDF_regex.toDF(newColumnNames: _*)

pokemonDF_regex.write
  .option("header", "true")
  .saveAsTable("pokemonStatsRegex")

display(pokemonDF_regex)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>Crea una nueva tabla a partir de pokemonstats teniendo en cuenta que la mayor parte de las consultas se realizan filtrando por generación de Pokémon.</h5>

// COMMAND ----------

 def createTableByGeneration(generation: Int): DataFrame = {
  val generation1DF = pokemonDF.filter(s"Generation = $generation")

  generation1DF.write
    .mode("overwrite")
    .saveAsTable(s"pokemonGeneration$generation")

    val pokemonGen1 = spark.table(s"pokemonGeneration$generation")
    return pokemonGen1;
 }
display(createTableByGeneration(6))


// COMMAND ----------

// MAGIC %md
// MAGIC <h5>Crea una nueva tabla a partir de pokemonstats teniendo en cuenta que la mayor parte de las consultas se realizan filtrando por generación de Pokémon, tipoA y tipoB .</h5>

// COMMAND ----------

def createTableByGenerationAndTypes(typeA: String, typeB: String, generation: Int): DataFrame = {
  val generationDF = pokemonDF.filter(s"Generation = $generation AND Type1 = '$typeA' AND Type2 = '$typeB'")

  generationDF.write
    .mode("overwrite")
    .saveAsTable(s"pokemonGeneration$generation" + s"Type1$typeA" + s"Type2$typeB")

  val pokemonGen = spark.table(s"pokemonGeneration$generation" + s"Type1$typeA" + s"Type2$typeB")
  pokemonGen
}

display(createTableByGenerationAndTypes("Grass", "Fighting", 6))


// COMMAND ----------

// MAGIC %md
// MAGIC <h5>¿Qué generación es más poderosa?</h5>

// COMMAND ----------

val powerByGenerationDF = pokemonDF.groupBy("Generation")
  .agg(sum("Total").alias("TotalPower"))
  .orderBy(desc("TotalPower"))

  display(powerByGenerationDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>¿Qué generacion tiene el mayor número de Pokémon de tipo fuego? ¿Cuántos tiene?</h5>

// COMMAND ----------

val fireTypeByGenerationDF = pokemonDF.filter("Type1 = 'Fire' OR Type2 = 'Fire'")
  .groupBy("Generation")
  .agg(count("Name").alias("FireTypeCount"))
  .sort(desc("FireTypeCount"))

  display(fireTypeByGenerationDF)
