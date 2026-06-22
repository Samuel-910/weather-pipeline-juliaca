import nbformat as nbf

nb = nbf.v4.new_notebook()

# Introducción Breve
cell_intro = nbf.v4.new_markdown_cell("""# ⛅ Pipeline de Datos Climáticos en Streaming (Juliaca, PE)
Este notebook documenta y ejecuta de forma interactiva el pipeline de streaming. En las siguientes secciones veremos cómo interactúan las distintas piezas del ecosistema (API, Almacenamiento Local, Kafka, Zookeeper, Spark, Prometheus y Dashboard).""")

cell_imports = nbf.v4.new_code_cell("""# ==============================================================================
# CELDA 1: IMPORTACIONES Y CONFIGURACIÓN INICIAL
# ==============================================================================
import os
import sys
import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

# Importaciones locales y de observabilidad
from prometheus_client import start_http_server, Counter, Gauge
from data.almacenamiento import guardar

load_dotenv()
# Obtenemos las credenciales y configuración del archivo .env
# os.getenv("NOMBRE", "VALOR_POR_DEFECTO") intenta leer la variable, y si no existe usa el default
API_KEY = os.getenv("API_KEY", "0de64add0be9c3db867244737a183207") # Fallback a tu llave por defecto
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-events")
CIUDAD = os.getenv("CIUDAD", "Juliaca")
PAIS = os.getenv("PAIS", "PE")
INTERVALO = 5  # Segundos de pausa

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
print(f"Configuración cargada. Broker: {BROKER}, Tópico: {TOPIC}, Ciudad: {CIUDAD},{PAIS}")""")

# Explicación de la API
cell_api_md = nbf.v4.new_markdown_cell("""## 🌍 1. Extracción de Datos: OpenWeatherMap API
Antes de enviar nada, necesitamos los datos en bruto. Aquí es donde actúa nuestra función de extracción. 

**¿Qué hace esta parte exacta?**
* Se conecta mediante HTTP GET a la **API pública de OpenWeatherMap**.
* La API nos devuelve un archivo JSON gigante y desordenado. Nuestra función lo limpia, selecciona solo lo que nos importa y le estampa la **fecha y hora exacta** (`timestamp`).""")

cell_api_code = nbf.v4.new_code_cell("""# ==============================================================================
# CELDA 2: EXTRACCIÓN DE DATOS DESDE LA API
# ==============================================================================
def obtener_clima():
    params = {
        "q": f"{CIUDAD},{PAIS}",
        "appid": API_KEY,
        "units": "metric",
        "lang": "es",
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    evento = {
        "timestamp": datetime.now().astimezone().isoformat(),
        "ciudad": data["name"],
        "pais": data["sys"]["country"],
        "latitud": data["coord"]["lat"],
        "longitud": data["coord"]["lon"],
        "temperatura": round(data["main"]["temp"], 2),
        "sensacion_termica": round(data["main"]["feels_like"], 2),
        "temp_min": round(data["main"]["temp_min"], 2), 
        "temp_max": round(data["main"]["temp_max"], 2),
        "humedad": data["main"]["humidity"],
        "presion": data["main"]["pressure"],
        "descripcion": data["weather"][0]["description"],
        "nubosidad": data["clouds"]["all"],
        "velocidad_viento": round(data["wind"]["speed"], 2),
        "visibilidad": data.get("visibility", 0),
        "hora_dia": datetime.now().hour,
        "dia_semana": datetime.now().weekday(),
    }
    return evento""")

# Explicación del Almacenamiento Local (DB y Parquet)
cell_storage_md = nbf.v4.new_markdown_cell("""## 💾 2. Almacenamiento Local e Histórico (DB y Parquet)
Aunque la magia está en el streaming de Kafka, nuestro pipeline es robusto y guarda un registro permanente de cada medición. En el código usaremos la función `guardar()` que viene de `data/almacenamiento.py`.

**¿Dónde y cómo se guardan?**
* **Base de Datos (SQLite):** Cada evento se inserta en `weather_juliaca.db`. Esto nos permite realizar consultas rápidas usando código SQL tradicional para revisar históricos.
* **Archivos Parquet:** El script acumula lotes de eventos (ej. cada 50) y los comprime en formato `.parquet`. Este es un formato "columnar" altamente optimizado que usa el ecosistema Big Data (ideal si luego queremos entrenar algoritmos de *Machine Learning* con PySpark MLlib).
* **CSV:** También guarda una copia en texto plano para rápida validación en Excel o Pandas.""")


# Explicación de Prometheus y Grafana
cell_prom_md = nbf.v4.new_markdown_cell("""## 📈 3. Observabilidad: Prometheus y Grafana
En infraestructuras de datos, es vital saber si el sistema funciona bien sin tener que estar mirando la consola.

**¿Para qué sirven Prometheus y Grafana en nuestro pipeline?**
* **Prometheus:** Es nuestra base de datos de métricas. Funciona **"raspando" (leyendo)** métricas desde nuestro código. En la celda de abajo levantaremos un puerto interno (8000) para que Prometheus recopile los aciertos y errores.
* **Grafana:** Prometheus es excelente guardando números, pero feo para visualizarlos. Grafana se conecta a Prometheus y nos permite crear **Paneles de Control (Dashboards)** hermosos y dinámicos. Hemos pre-configurado Grafana para que te muestre gráficas en tiempo real de los mensajes enviados y los errores detectados.""")


# Explicación de Zookeeper y Kafka
cell_kafka_md = nbf.v4.new_markdown_cell("""## 📬 4. El Productor, Kafka y Zookeeper
Ahora sí transportaremos los datos de forma segura. Aquí entran Kafka y Zookeeper.

**¿Qué hace Zookeeper?**
* Zookeeper es el "director de orquesta". Mantiene el clúster coordinado. Kafka no puede vivir sin Zookeeper, ya que él guarda la información de qué nodos están vivos y dónde están los tópicos.

**¿Qué hace Kafka?**
* Es nuestro **Message Broker**. Actúa como un buzón gigante de altísimo rendimiento. Toma nuestro JSON (convertido a bytes) y lo mete en la cola (`weather-events`), a la espera del consumidor (Spark).""")

cell_kafka_code = nbf.v4.new_code_cell("""# ==============================================================================
# CELDA 3: CONEXIÓN, MÉTRICAS, ALMACENAMIENTO Y ENVÍO A KAFKA
# ==============================================================================
# 1. Definir métricas de Prometheus
try:
    eventos_enviados = Counter('weather_events_total', 'Número total de eventos del clima enviados exitosamente')
    errores_envio = Counter('weather_events_errors_total', 'Número total de errores al intentar enviar eventos')
    temp_gauge = Gauge('weather_temperature_celsius', 'Temperatura actual en grados Celsius')
    humedad_gauge = Gauge('weather_humidity_percent', 'Humedad actual en porcentaje')
    viento_gauge = Gauge('weather_wind_speed_mps', 'Velocidad del viento en metros por segundo')
    presion_gauge = Gauge('weather_pressure_hpa', 'Presión atmosférica en hPa')
    start_http_server(8000)
    print("Servidor de métricas de Prometheus iniciado en el puerto 8000")
except Exception as e:
    pass # Ya estaba corriendo

def conectar_kafka():
    print(f"Conectando a Kafka en {BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )
    return producer

producer = conectar_kafka()
import threading

def ejecutar_productor():
    print(f"\nIniciando envío CONTINUO simulado para {CIUDAD}, {PAIS} en segundo plano...")
    while True:
        try:
            evento = obtener_clima()
            
            # 2. Guardar en SQLite, CSV y Parquet
            guardar(evento)
            
            # 3. Enviar al tópico Kafka
            future = producer.send(TOPIC, value=evento)
            future.get(timeout=10) 
            
            # Incrementar métrica de Prometheus
            eventos_enviados.inc()
            temp_gauge.set(evento['temperatura'])
            humedad_gauge.set(evento['humedad'])
            viento_gauge.set(evento['velocidad_viento'])
            presion_gauge.set(evento['presion'])
            
            ts = evento["timestamp"][11:19]
            print(f"[{ts}] Guardado en DB/Parquet y Enviado a Kafka -> temp={evento['temperatura']}°C")
        except Exception as e:
            print(f"Error: {e}")
            errores_envio.inc()
            
        time.sleep(INTERVALO)

# Ejecutar el productor en un hilo secundario para no bloquear el notebook
hilo_productor = threading.Thread(target=ejecutar_productor, daemon=True)
hilo_productor.start()
print("Productor ejecutándose en segundo plano. Puedes continuar con la siguiente celda.")""")


# Explicación de Spark
cell_spark_md = nbf.v4.new_markdown_cell("""## 🧠 5. Consumidor: Apache Spark Structured Streaming
Los datos ya están en Kafka y guardados localmente. Ahora analizaremos el flujo (stream) en tiempo real. 

**¿Qué hace Apache Spark aquí?**
* Spark se conecta a Kafka en modo de "escucha perpetua".
* **Agrupación Temporal (Windowing):** Mete los datos en "ventanas" (ej. bloques de 5 minutos) para sacar el promedio.
* **Manejo de Retrasos (Watermarking):** Usa `withWatermark` para lidiar con mensajes que llegan tarde por mala conexión, re-calculando el promedio viejo automáticamente.
* Guardará la tabla agrupada en la **memoria RAM** (`format("memory")`) para nosotros consultarla.""")

cell_spark_code = nbf.v4.new_code_cell("""# ==============================================================================
# CELDA 4: PROCESAMIENTO ANALÍTICO CON SPARK
# ==============================================================================
import os
BASE_DIR = os.path.abspath("")
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
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, max, min, count, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Inicializar Spark
spark = SparkSession.builder \\
    .appName("WeatherStreaming") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \\
    .config("spark.sql.shuffle.partitions", "2") \\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SCHEMA = StructType([
    StructField("timestamp", StringType()), StructField("ciudad", StringType()), StructField("pais", StringType()),
    StructField("latitud", DoubleType()), StructField("longitud", DoubleType()), StructField("temperatura", DoubleType()),
    StructField("sensacion_termica", DoubleType()), StructField("temp_min", DoubleType()), StructField("temp_max", DoubleType()),
    StructField("humedad", IntegerType()), StructField("presion", IntegerType()), StructField("descripcion", StringType()),
    StructField("nubosidad", IntegerType()), StructField("velocidad_viento", DoubleType()), StructField("visibilidad", IntegerType()),
    StructField("hora_dia", IntegerType()), StructField("dia_semana", IntegerType())
])

# 1. Leer de Kafka
df_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER) \\
    .option("subscribe", TOPIC).option("startingOffsets", "earliest").load()

# 2. Parsear JSON
df = df_raw.select(from_json(col("value").cast("string"), SCHEMA).alias("d")).select("d.*") \\
           .withColumn("event_time", to_timestamp(col("timestamp")))

# 3. Analítica con Ventanas de Tiempo (5 mins) y Watermark (2 mins)
df_agg = df.withWatermark("event_time", "2 minutes") \\
    .groupBy(window(col("event_time"), "5 minutes"), col("hora_dia"), col("ciudad")) \\
    .agg(
        spark_round(avg("temperatura"), 2).alias("temp_promedio"),
        spark_round(avg("humedad"), 1).alias("humedad_promedio"),
        spark_round(avg("velocidad_viento"), 2).alias("viento_promedio"),
        count("*").alias("total_eventos")
    )

# 4. Guardar en memoria continuamente
query = df_agg.writeStream.outputMode("update").format("memory").queryName("weather_aggs") \\
    .trigger(processingTime="5 seconds").start()

print("Spark Stream iniciado. Calculando promedios en tiempo real...")""")

# Explicación Final y Dashboard
cell_dash_md = nbf.v4.new_markdown_cell("""## 📊 6. Consulta Final y El Dashboard
Finalmente consultamos la tabla virtual de Spark para ver los promedios.

**¿Y qué papel juega el Dashboard en el proyecto general?**
* Aunque aquí usemos Spark SQL para ver tablas, en el proyecto existe una aplicación web (`dashboard/app.py`).
* Es una app en **Flask** que lee de Kafka y usa tecnología **SSE (Server-Sent Events)** para empujar gráficas y mapas de calor al navegador web de forma automática cada vez que se detecta un cambio. Es la cara visual de nuestra arquitectura.""")

cell_dash_code = nbf.v4.new_code_cell("""# ==============================================================================
# CELDA 5: RESULTADOS Y SQL
# ==============================================================================
import time
print("Esperando 10 segundos para que Spark llene la tabla virtual...")
time.sleep(10)

print("\\n--- RESULTADOS AGREGADOS SPARK (VENTANAS DE 5 MINUTOS) ---")
spark.sql("SELECT * FROM weather_aggs ORDER BY window DESC").show(truncate=False)

# Descomentar para detener Spark al terminar
# query.stop()""")

nb['cells'] = [
    cell_intro, cell_imports, 
    cell_api_md, cell_api_code,
    cell_storage_md,
    cell_prom_md, 
    cell_kafka_md, cell_kafka_code, 
    cell_spark_md, cell_spark_code, 
    cell_dash_md, cell_dash_code
]

with open("Weather_Pipeline_Juliaca.ipynb", "w", encoding="utf-8") as f:
    nbf.write(nb, f)

print("Notebook actualizado con almacenamiento (Parquet/DB).")
