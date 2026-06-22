"""
S7 — Spark Structured Streaming
Lee desde Kafka, aplica ventanas + watermarking
y genera agregaciones para el mapa de calor
"""

import os

# Configurar HADOOP_HOME para Spark en Windows
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "hadoop")
os.environ["hadoop.home.dir"] = os.path.join(BASE_DIR, "hadoop")

# Argumentos de compatibilidad para ejecutar Spark en Java 17 o Java 21
java_opts = (
    "-XX:+IgnoreUnrecognizedVMOptions "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --driver-java-options "{java_opts}" pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    window, avg, max, min, count, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
from dotenv import load_dotenv

load_dotenv()

BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC      = os.getenv("KAFKA_TOPIC",  "weather-events")
CHECKPOINT = "/tmp/checkpoint/weather"


SCHEMA = StructType([
    StructField("timestamp",         StringType()),
    StructField("ciudad",            StringType()),
    StructField("pais",              StringType()),
    StructField("latitud",           DoubleType()),
    StructField("longitud",          DoubleType()),
    StructField("temperatura",       DoubleType()),
    StructField("sensacion_termica", DoubleType()),
    StructField("temp_min",          DoubleType()),
    StructField("temp_max",          DoubleType()),
    StructField("humedad",           IntegerType()),
    StructField("presion",           IntegerType()),
    StructField("descripcion",       StringType()),
    StructField("nubosidad",         IntegerType()),
    StructField("velocidad_viento",  DoubleType()),
    StructField("visibilidad",       IntegerType()),
    StructField("hora_dia",          IntegerType()),
    StructField("dia_semana",        IntegerType()),
])


def crear_spark():
    return SparkSession.builder \
        .appName("WeatherStreaming-Juliaca") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
        ) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def leer_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parsear(df_raw):
    return df_raw \
        .select(
            from_json(col("value").cast("string"), SCHEMA).alias("d")
        ) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))


def agregar_ventana(df):
    """
    Ventana de 5 minutos con watermark de 2 minutos.
    Agrupa por hora del día para el mapa de calor.
    """
    return df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("hora_dia"),
            col("ciudad"),
        ) \
        .agg(
            spark_round(avg("temperatura"),       2).alias("temp_promedio"),
            spark_round(max("temperatura"),       2).alias("temp_max"),
            spark_round(min("temperatura"),       2).alias("temp_min"),
            spark_round(avg("humedad"),           1).alias("humedad_promedio"),
            spark_round(avg("velocidad_viento"),  2).alias("viento_promedio"),
            spark_round(avg("sensacion_termica"), 2).alias("sensacion_promedio"),
            count("*").alias("total_eventos"),
        )


def main():
    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Pipeline iniciado — Juliaca clima streaming")
    print(f"Broker: {BROKER} | Tópico: {TOPIC}")
    print("-" * 60)

    df_raw  = leer_kafka(spark)
    df      = parsear(df_raw)
    df_agg  = agregar_ventana(df)

    query = df_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 50) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", CHECKPOINT) \
        .start()

    print("Stream activo. Esperando datos de Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
