# Componente de Procesamiento: Spark Streaming (S7)

El componente **S7** realiza el procesamiento distribuido en tiempo real. Utiliza **Apache Spark Structured Streaming** para leer datos del tópico de Kafka, aplicar análisis en ventanas de tiempo móviles y mostrar los resultados consolidados por consola.

---

## 1. Configuración de la Sesión y Compatibilidad (JVM)

El script [streaming_job.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/s7_spark/streaming_job.py) configura la sesión de PySpark. En sistemas Windows o versiones de Java recientes (Java 17 o Java 21), la máquina virtual de Java (JVM) restringe ciertos accesos reflexivos internos que Spark requiere. 

Para solventarlo, se inyectan argumentos JVM específicos en las variables del sistema de PySpark al arrancar:

```python
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
    "--add-security.jgss/sun.security.krb5=ALL-UNNAMED"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --driver-java-options "{java_opts}" pyspark-shell'
```

Además, Spark carga el conector `spark-sql-kafka-0-10_2.12:3.4.0` para poder suscribirse a flujos de Kafka directamente.

---

## 2. Definición del Esquema e Ingesta

Los datos en el tópico `weather-events` viajan en formato binario JSON. En Spark, para poder realizar agregaciones sobre columnas, se debe declarar la estructura exacta (`Schema`) de los datos:

```python
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
```

El script parsea la cadena JSON y convierte el string `timestamp` a un formato nativo de fecha y hora (`TimestampType`) necesario para el cálculo de ventanas temporales:

```python
def parsear(df_raw):
    return df_raw \
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))
```

---

## 3. Ventanas de Tiempo y Watermarking

Para evitar el consumo desmedido de memoria por acumulación de datos históricos infinitos, Spark Streaming utiliza dos conceptos clave:

- **Watermarking (Marca de agua)**: Define cuánto tiempo está dispuesto a esperar Spark por eventos retrasados. En este pipeline, se establece en **2 minutos**. Si llega un mensaje cuya fecha y hora de evento (`event_time`) es mayor que la marca actual menos 2 minutos, se procesa; de lo contrario, se descarta.
- **Ventanas de Tiempo Móviles (Time Windows)**: Los datos se agrupan en bloques fijos de **5 minutos** basados en el tiempo del evento.

```python
def agregar_ventana(df):
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
```

Las agregaciones calculan promedios, valores mínimos y máximos de la temperatura y viento dentro de cada ventana.

---

## 4. Escritura en Modo Update

El flujo se escribe de vuelta a la consola utilizando el modo **`update`**:

```python
query = df_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint/weather") \
    .start()
```

- **Output Mode `"update"`**: Solo imprime las ventanas que han recibido nuevos eventos en los últimos 30 segundos, reduciendo la saturación de consola.
- **Trigger**: Se ejecuta una micro-evaluación cada 30 segundos (`trigger(processingTime="30 seconds")`).
- **Checkpointing**: Guarda el estado interno en `/tmp/checkpoint/weather` para garantizar tolerancia a fallos en el procesamiento.
