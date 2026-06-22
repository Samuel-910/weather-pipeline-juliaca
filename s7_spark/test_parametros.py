"""
S7 — Pruebas comparativas de parámetros
Mide latencia y throughput variando trigger y watermark
para llenar la tabla de métricas del informe (Sección 5)
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "weather-events")

SCHEMA = StructType([
    StructField("timestamp",   StringType()),
    StructField("ciudad",      StringType()),
    StructField("temperatura", DoubleType()),
    StructField("humedad",     IntegerType()),
    StructField("hora_dia",    IntegerType()),
])

PRUEBAS = [
    {"id": 1, "trigger": "10 seconds", "watermark": "1 minute",  "ventana": "5 minutes"},
    {"id": 2, "trigger": "30 seconds", "watermark": "2 minutes", "ventana": "5 minutes"},
    {"id": 3, "trigger": "60 seconds", "watermark": "5 minutes", "ventana": "10 minutes"},
]

resultados = []


def registrar_batch(df, epoch_id, config):
    inicio = time.time()
    n_filas = df.count()
    latencia = round(time.time() - inicio, 3)
    throughput = round(n_filas / max(latencia, 0.001), 2)

    print(
        f"[Prueba {config['id']}] epoch={epoch_id} "
        f"filas={n_filas} latencia={latencia}s throughput={throughput} ev/s"
    )
    resultados.append({
        "prueba":     config["id"],
        "trigger":    config["trigger"],
        "watermark":  config["watermark"],
        "ventana":    config["ventana"],
        "epoch":      epoch_id,
        "filas":      n_filas,
        "latencia_s": latencia,
        "throughput": throughput,
    })


def ejecutar_prueba(spark, config, duracion_seg=120):
    print(f"\n=== PRUEBA {config['id']} ===")
    print(f"Trigger: {config['trigger']} | Watermark: {config['watermark']} | Ventana: {config['ventana']}")

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    df = df_raw \
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    df_agg = df \
        .withWatermark("event_time", config["watermark"]) \
        .groupBy(
            window(col("event_time"), config["ventana"]),
            col("hora_dia"),
        ) \
        .agg(
            avg("temperatura").alias("temp_promedio"),
            count("*").alias("total_eventos"),
        )

    query = df_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, eid: registrar_batch(df, eid, config)) \
        .trigger(processingTime=config["trigger"]) \
        .option("checkpointLocation", f"/tmp/checkpoint/prueba_{config['id']}") \
        .start()

    time.sleep(duracion_seg)
    query.stop()


def imprimir_resultados():
    print("\n" + "=" * 70)
    print("TABLA COMPARATIVA DE MÉTRICAS (para Sección 5 del informe)")
    print("=" * 70)
    print(f"{'Prueba':<8} {'Trigger':<14} {'Watermark':<12} {'Latencia':<12} {'Throughput'}")
    print("-" * 70)
    for r in resultados:
        print(
            f"{r['prueba']:<8} {r['trigger']:<14} {r['watermark']:<12} "
            f"{r['latencia_s']}s{'':<7} {r['throughput']} ev/s"
        )
    print("=" * 70)


def main():
    spark = SparkSession.builder \
        .appName("WeatherStreaming-Pruebas") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
        ) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for config in PRUEBAS:
        ejecutar_prueba(spark, config, duracion_seg=120)

    imprimir_resultados()
    spark.stop()


if __name__ == "__main__":
    main()
