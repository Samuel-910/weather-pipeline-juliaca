import os
import time
import json
import logging
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "weather-events")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pipeline.metricas")

UMBRALES = {
    "latencia_max_s":     5.0,
    "error_rate_max_pct": 5.0,
    "backpressure_max_pct": 80.0,
    "throughput_min_ev_min": 0.5,
}

metricas_globales = {
    "total_eventos":   0,
    "total_errores":   0,
    "latencia_acum":   0.0,
    "batches":         0,
}


def registrar_batch(n_filas, latencia_s, errores=0):
    metricas_globales["total_eventos"] += n_filas
    metricas_globales["total_errores"] += errores
    metricas_globales["latencia_acum"] += latencia_s
    metricas_globales["batches"]       += 1

    error_rate = (
        metricas_globales["total_errores"] /
        max(metricas_globales["total_eventos"], 1) * 100
    )
    lat_prom = metricas_globales["latencia_acum"] / metricas_globales["batches"]
    
    # Simulación de rendimiento basado en el tiempo transcurrido
    throughput = metricas_globales["total_eventos"] / max(
        metricas_globales["batches"] * 0.5, 1
    )

    metrica = {
        "timestamp":       datetime.now().isoformat(),
        "batch_num":       metricas_globales["batches"],
        "filas_batch":     n_filas,
        "latencia_s":      round(latencia_s, 3),
        "latencia_prom_s": round(lat_prom, 3),
        "throughput_ev_m": round(throughput, 2),
        "error_rate_pct":  round(error_rate, 2),
        "total_eventos":   metricas_globales["total_eventos"],
    }

    log.info("METRICA | " + json.dumps(metrica))
    verificar_alertas(metrica)
    return metrica


def verificar_alertas(m):
    if m["latencia_s"] > UMBRALES["latencia_max_s"]:
        log.warning(
            f"ALERTA latencia | {m['latencia_s']}s supera umbral "
            f"{UMBRALES['latencia_max_s']}s"
        )

    if m["error_rate_pct"] > UMBRALES["error_rate_max_pct"]:
        log.error(
            f"ALERTA errores | tasa {m['error_rate_pct']}% supera umbral "
            f"{UMBRALES['error_rate_max_pct']}%"
        )

    if m["throughput_ev_m"] < UMBRALES["throughput_min_ev_min"]:
        log.warning(
            f"ALERTA throughput | {m['throughput_ev_m']} ev/min por debajo "
            f"del mínimo {UMBRALES['throughput_min_ev_min']}"
        )


def medir_backpressure():
    """
    Mide el lag del consumer Kafka.
    Un lag alto indica backpressure: Kafka acumula más de lo que Spark consume.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=BROKER)
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            group_id="spark-streaming-juliaca",
            auto_offset_reset="latest",
        )
        particiones = consumer.partitions_for_topic(TOPIC) or {0}
        lag_total = 0
        for p in particiones:
            tp = __import__("kafka").TopicPartition(TOPIC, p)
            end_offsets    = consumer.end_offsets([tp])
            current_offset = consumer.position(tp)
            lag_total += end_offsets[tp] - current_offset

        consumer.close()
        admin.close()
        pct = min(lag_total / max(lag_total + 10, 1) * 100, 100)

        if pct > UMBRALES["backpressure_max_pct"]:
            log.error(f"ALERTA backpressure | lag={lag_total} msgs ({pct:.1f}%)")
        else:
            log.info(f"Backpressure OK | lag={lag_total} msgs ({pct:.1f}%)")

        return {"lag_mensajes": lag_total, "backpressure_pct": round(pct, 1)}

    except Exception as e:
        log.warning(f"No se pudo medir backpressure: {e}")
        return {"lag_mensajes": 0, "backpressure_pct": 0.0}


def resumen_operativo():
    b = metricas_globales["batches"]
    if b == 0:
        return
    print("\n" + "=" * 60)
    print("RESUMEN OPERATIVO DEL PIPELINE")
    print("=" * 60)
    print(f"  Batches procesados : {b}")
    print(f"  Total eventos      : {metricas_globales['total_eventos']}")
    print(f"  Total errores      : {metricas_globales['total_errores']}")
    print(f"  Latencia promedio  : {metricas_globales['latencia_acum']/b:.2f}s")
    print(f"  Log guardado en    : {LOG_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    log.info("Monitor de métricas iniciado en modo continuo")
    log.info(f"Umbrales activos: {json.dumps(UMBRALES)}")

    try:
        # Bucle infinito para leer TODO el streaming sin detenerse
        while True:
            # 1. Medimos el estado de Kafka (Lag / Backpressure)
            bp = medir_backpressure()
            
            # 2. Registramos el batch actual. 
            # NOTA: En un entorno real, estos datos vendrían de la ejecución de Spark.
            # Aquí simulamos un consumo constante de 10 filas y latencia aleatoria cercana a 2s para el test.
            import random
            lat_simulada = round(random.uniform(1.0, 5.5), 2) 
            errores_simulados = 1 if lat_simulada > 5.0 else 0 # Simular error si tarda mucho
            
            registrar_batch(n_filas=10, latencia_s=lat_simulada, errores=errores_simulados)
            
            # Frecuencia de muestreo (revisa el pipeline cada 5 segundos)
            time.sleep(5)
            
    except KeyboardInterrupt:
        # Al presionar Ctrl + C en la terminal, detiene el bucle limpiamente y muestra el resumen
        log.info("Monitoreo finalizado por el usuario.")
        resumen_operativo()