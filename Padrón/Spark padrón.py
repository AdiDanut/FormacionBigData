# Databricks notebook source
# MAGIC %md
# MAGIC <h3>6- Un poquito de Spark.</h3>
# MAGIC <p>La siguiente sección de la práctica se abordará si ya se tienen suficientes conocimientos de 
# MAGIC Spark, en concreto de el manejo de DataFrames, y el manejo de tablas de Hive a través de 
# MAGIC Spark.sql.</p>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>• 6.1) Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el 
# MAGIC csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que 
# MAGIC ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y 
# MAGIC que infiera el esquema</h5>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

# COMMAND ----------

spark = SparkSession.builder.appName("ImportarCSV").getOrCreate()

opciones_csv = {
    "header": "true",
    "delimiter": ";",
    "quote": '"',
    "escape": '"',
    "ignoreLeadingWhiteSpace": "true",
    "ignoreTrailingWhiteSpace": "true",
    "emptyValue": "0"
}

ruta_csv = "/FileStore/tables/estadisticas202402.csv"

datos_spark = spark.read.options(**opciones_csv).csv(ruta_csv)

for columna in datos_spark.columns:
    datos_spark = datos_spark.withColumn(columna, trim(col(columna)))

display(datos_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>• 6.3)
# MAGIC Enumera todos los barrios diferentes.</h5>

# COMMAND ----------

num_barrios_distintos = datos_spark.select("COD_BARRIO").distinct().count()
print(num_barrios_distintos)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> 6.4)
# MAGIC Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
# MAGIC diferentes que hay.</h5>

# COMMAND ----------

datos_spark.createOrReplaceTempView("padron")
barrios_distintos_view = spark.sql("SELECT COUNT(DISTINCT COD_BARRIO) as num_barrios FROM padron").collect()[0]['num_barrios']

print("Número de barrios distintos:", barrios_distintos_view)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> 6.5)
# MAGIC Crea una nueva columna que muestre la longitud de los campos de la columna 
# MAGIC DESC_DISTRITO y que se llame "longitud".</h5>

# COMMAND ----------

datos_spark = datos_spark.withColumn("longitud", length("DESC_DISTRITO"))

display(datos_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>• 6.6)
# MAGIC Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla</h5>

# COMMAND ----------

datos_spark = datos_spark.withColumn("nueva_columna", lit(5))
display(datos_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>• 6.7)
# MAGIC Borra esta columna.</h5>

# COMMAND ----------

datos_spark = datos_spark.drop("nueva_columna")
display(datos_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> 6.8)
# MAGIC Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO</h5>

# COMMAND ----------

datos_spark = datos_spark.repartition("DESC_DISTRITO", "DESC_BARRIO")
display(datos_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
# MAGIC de los rdds almacenados.</h5>

# COMMAND ----------

datos_spark.cache();
datos_spark.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Lanza una consulta contra el DF resultante en la que muestre el número total de 
# MAGIC "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" 
# MAGIC para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en 
# MAGIC aparecer en el show. Los resultados deben estar ordenados en orden de más a menos 
# MAGIC según la columna "extranjerosmujeres" y desempatarán por la columna 
# MAGIC "extranjeroshombres"</h5>

# COMMAND ----------

resultados = datos_spark.withColumn("espanoleshombres", col("espanoleshombres").cast("int")). \
    withColumn("espanolesmujeres", col("espanolesmujeres").cast("int")). \
    withColumn("extranjeroshombres", col("extranjeroshombres").cast("int")). \
    withColumn("extranjerosmujeres", col("extranjerosmujeres").cast("int")). \
    groupBy("DESC_DISTRITO", "DESC_BARRIO"). \
    agg(sum("espanoleshombres").alias("Total_EspanolesHombres"),
        sum("espanolesmujeres").alias("Total_EspanolesMujeres"),
        sum("extranjeroshombres").alias("Total_ExtranjerosHombres"),
        sum("extranjerosmujeres").alias("Total_ExtranjerosMujeres")). \
    orderBy(col("Total_ExtranjerosMujeres").desc(), col("Total_ExtranjerosHombres").desc())

display(resultados)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>6.13)
# MAGIC Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).</h5>

# COMMAND ----------

ventana = Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")

resultados = datos_spark.withColumn("espanoleshombres", col("espanoleshombres").cast("int")). \
    withColumn("espanolesmujeres", col("espanolesmujeres").cast("int")). \
    withColumn("extranjeroshombres", col("extranjeroshombres").cast("int")). \
    withColumn("extranjerosmujeres", col("extranjerosmujeres").cast("int")). \
    withColumn("Total_EspanolesHombres", sum("espanoleshombres").over(ventana)). \
    withColumn("Total_EspanolesMujeres", sum("espanolesmujeres").over(ventana)). \
    withColumn("Total_ExtranjerosHombres", sum("extranjeroshombres").over(ventana)). \
    withColumn("Total_ExtranjerosMujeres", sum("extranjerosmujeres").over(ventana)). \
    select("DESC_DISTRITO", "DESC_BARRIO", "Total_EspanolesHombres", "Total_EspanolesMujeres", 
           "Total_ExtranjerosHombres", "Total_ExtranjerosMujeres"). \
    distinct(). \
    orderBy(col("Total_ExtranjerosMujeres").desc(), col("Total_ExtranjerosHombres").desc())

display(resultados)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>• 6.14)
# MAGIC Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
# MAGIC contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y 
# MAGIC en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente 
# MAGIC CENTRO, BARAJAS y RETIRO y deben figurar como columnas .</h5>

# COMMAND ----------

distritos_seleccionados = ["CENTRO", "BARAJAS", "RETIRO"]
datos_filtrados = datos_spark.filter(col("DESC_DISTRITO").isin(distritos_seleccionados))
datos_filtrados = datos_filtrados.withColumn("espanolesmujeres", datos_spark["espanolesmujeres"].cast("double"))
tabla_contingencia = datos_filtrados.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO").sum("espanolesmujeres")
display(tabla_contingencia)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>6.15)
# MAGIC Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
# MAGIC de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
# MAGIC cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
# MAGIC condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.</h5>

# COMMAND ----------

sum_espanolesmujeres_total = tabla_contingencia.groupBy().sum("CENTRO", "BARAJAS", "RETIRO").collect()[0]

tabla_contingencia = tabla_contingencia.withColumn("porcentaje_CENTRO", round(col("CENTRO") / sum_espanolesmujeres_total["sum(CENTRO)"] * 100, 2))
tabla_contingencia = tabla_contingencia.withColumn("porcentaje_BARAJAS", round(col("BARAJAS") / sum_espanolesmujeres_total["sum(BARAJAS)"] * 100, 2))
tabla_contingencia = tabla_contingencia.withColumn("porcentaje_RETIRO", round(col("RETIRO") / sum_espanolesmujeres_total["sum(RETIRO)"] * 100, 2))


display(tabla_contingencia)

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>6.16)
# MAGIC Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un 
# MAGIC directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba 
# MAGIC que es la esperada</h5>

# COMMAND ----------

datos_spark.write.partitionBy("DESC_DISTRITO", "DESC_BARRIO").csv("/FileStore/tables/datos_spark_csv")

# COMMAND ----------

datos_spark.write.partitionBy("DESC_DISTRITO", "DESC_BARRIO").parquet("/FileStore/tables/datos_spark_parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Por último, prueba a hacer los ejercicios sugeridos en la parte de Hive con el csv "Datos 
# MAGIC Padrón" (incluyendo la importación con Regex) utilizando desde Spark EXCLUSIVAMENTE 
# MAGIC sentencias spark.sql, es decir, importar los archivos desde local directamente como tablas
# MAGIC de Hive y haciendo todas las consultas sobre estas tablas sin transformarlas en ningún 
# MAGIC momento en DataFrames ni DataSets.</h5>

# COMMAND ----------

##No puedo ejecutar sentencias SQL de Hive en Databricks, ya que databricks utiliza su propio motor llamado Databricks SQL.
