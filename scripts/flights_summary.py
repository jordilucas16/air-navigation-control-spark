import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr, col, unix_timestamp
from pyspark.sql.types import TimestampType
import time

# Creem Spark Session
spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "1")

impresiones = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1").option("numPartitions", "1").load()
    .selectExpr("value AS idAnuncio", "timestamp AS tiempoImpresion")
)

# Creem les columnes idAnunci i tempsClic amb 1 segon d' interval despres de tiempoImpresion 
# i seleccionem de manera aleatòria només el 20% de files.
clics = (
  impresiones
    .withColumn("idAnunci", expr("idAnuncio"))
    .withColumn("tempsClic", expr("tiempoImpresion + INTERVAL 1 second"))
    .filter(rand() < 0.2)  # Seleccionem de manera aleatòria menys del 20%
    .drop("idAnuncio", "tiempoImpresion")
)


# Combinem amb join els dos streams de dades i afegim watermark per no acceptar dades amb >15 seg. de retard
# i afegim la columna deltaT amb la diferència de temps en segons entre el clic i la impressió del anunci.
combinacio = (
    impresiones.join(clics, impresiones.idAnuncio == clics.idAnunci)
    .withWatermark("tiempoImpresion", "15 seconds")
    .withWatermark("tempsClic", "15 seconds")
    .select(
        col("idAnuncio"), 
        col("tiempoImpresion"), 
        col("tempsClic"),
        (unix_timestamp("tempsClic") - unix_timestamp("tiempoImpresion")).alias("deltaT")
    )
)

resCombinacio = (
  combinacio
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime='5 seconds')
    .start()
)

resCombinacio.awaitTermination()
#resClics.awaitTermination()