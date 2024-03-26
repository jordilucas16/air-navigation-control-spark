import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode,from_json,schema_of_json,lit, col, from_unixtime, sin, cos, sqrt, atan2, toRadians, pow
#from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

ejemplo='{"ac": [{"flight": "RYR80XN ","lat": 40.783493,"lon": -9.551697, "alt_baro": 37000,"category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'

encabezados = ['flight','lat','lon','alt_baro','category']  

# Limits of area de Catalunya
top_left = (42.924299, 0.500251)
bottom_right = (40.294028, 3.567923)

# Define the Haversine formula as a function
# https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine_distance(lat1, lon1, lat2, lon2):
    radius = 6371  # Earth radius in kilometers
    delta_lat = toRadians(lat2 - lat1)
    delta_lon = toRadians(lon2 - lon1)
    a = pow(sin(delta_lat / 2), 2) + cos(toRadians(lat1)) * cos(toRadians(lat2)) * pow(sin(delta_lon / 2), 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = radius * c
    return distance


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("STRUCTURED STREAMING") \
    .getOrCreate()


# Socket port and host 
host = "localhost"
puerto = 10029


# Load the stream from the socket
flujo = spark.readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", puerto) \
    .load()


# StructType equivalent to schema_of_json
esquema_json = schema_of_json(ejemplo)
datos_json = flujo.select(from_json(col("value"), esquema_json).alias("datos"))

aviones_con_now = datos_json.select(explode(col("datos.ac")).alias("avion"), col("datos.now").alias("now"))

# Create DataFrame with specified columns + 'now' field
df_aviones = aviones_con_now.select([col("avion." + header).alias(header) for header in encabezados] + [col("now")])

# Filter inside Catalunya area, add column 'timestamp' and remove column 'now'
df_filtered = df_aviones.filter(
    (col("lat") <= top_left[0]) & 
    (col("lat") >= bottom_right[0]) &
    (col("lon") >= top_left[1]) & 
    (col("lon") <= bottom_right[1])
).withColumn("timestamp", from_unixtime(col("now")/1000)).drop(col("now"))

# Assuming df is the DataFrame with columns 'lat' and 'lon'
# Coordinates for the airports
barcelona_airport = (41.2971, 2.0833)
tarragona_airport = (41.1474, 1.1672)
girona_airport = (41.9009, 2.7606)

# Distance calculation
df_filtered = df_filtered.withColumn("distance_to_barcelona", haversine_distance(col("lat"), col("lon"), lit(barcelona_airport[0]), lit(barcelona_airport[1])))
df_filtered = df_filtered.withColumn("distance_to_tarragona", haversine_distance(col("lat"), col("lon"), lit(tarragona_airport[0]), lit(tarragona_airport[1])))
df_filtered = df_filtered.withColumn("distance_to_girona", haversine_distance(col("lat"), col("lon"), lit(girona_airport[0]), lit(girona_airport[1])))

# Groupby category desc if necessary
#df_grouped = df_filtered.groupBy("category").count().orderBy(col("count").desc())


# Start the stream
resultado = df_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

resultado.awaitTermination()
