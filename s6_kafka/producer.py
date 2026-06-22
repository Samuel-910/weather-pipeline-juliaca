"""
S6 — Productor Kafka
Consume OpenWeatherMap API para Juliaca, PE
y publica eventos JSON en el tópico weather-events
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from data.almacenamiento import guardar

load_dotenv()

API_KEY      = os.getenv("API_KEY")
BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = os.getenv("KAFKA_TOPIC",  "weather-events")
CIUDAD       = os.getenv("CIUDAD",       "Juliaca")
PAIS         = os.getenv("PAIS",         "PE")
INTERVALO    = int(os.getenv("INTERVALO_SEGUNDOS", 60))

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def conectar_kafka():
    print(f"Conectando a Kafka en {BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )
    print("Conexión exitosa.")
    return producer


def obtener_clima():
    params = {
        "q":     f"{CIUDAD},{PAIS}",
        "appid": API_KEY,
        "units": "metric",
        "lang":  "es",
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    evento = {
        "timestamp":        datetime.now().astimezone().isoformat(),
        "ciudad":           data["name"],
        "pais":             data["sys"]["country"],
        "latitud":          data["coord"]["lat"],
        "longitud":         data["coord"]["lon"],
        "temperatura":      round(data["main"]["temp"], 2),
        "sensacion_termica":round(data["main"]["feels_like"], 2),
        "temp_min":         round(data["main"]["temp_min"], 2),
        "temp_max":         round(data["main"]["temp_max"], 2),
        "humedad":          data["main"]["humidity"],
        "presion":          data["main"]["pressure"],
        "descripcion":      data["weather"][0]["description"],
        "nubosidad":        data["clouds"]["all"],
        "velocidad_viento": round(data["wind"]["speed"], 2),
        "visibilidad":      data.get("visibility", 0),
        "hora_dia":         datetime.now().hour,
        "dia_semana":       datetime.now().weekday(),
    }
    return evento


def main():
    producer = conectar_kafka()
    print(f"\nIniciando stream para {CIUDAD}, {PAIS}")
    print(f"Tópico: {TOPIC} | Intervalo: {INTERVALO}s\n")
    print("-" * 60)

    # 1. Configurar y arrancar servidor de Prometheus
    from prometheus_client import start_http_server, Counter, Gauge
    import sqlite3
    from data.almacenamiento import DB_PATH
    
    eventos_total = Gauge('weather_events_total', 'Número total de eventos del clima guardados históricamente')
    errores_envio = Counter('weather_events_errors_total', 'Número total de errores al intentar enviar eventos')
    temp_gauge = Gauge('weather_temperature_celsius', 'Temperatura actual en grados Celsius')
    humedad_gauge = Gauge('weather_humidity_percent', 'Humedad actual en porcentaje')
    viento_gauge = Gauge('weather_wind_speed_mps', 'Velocidad del viento en metros por segundo')
    presion_gauge = Gauge('weather_pressure_hpa', 'Presión atmosférica en hPa')
    
    try:
        start_http_server(8000)
        print("Servidor de métricas de Prometheus iniciado en el puerto 8000")
    except Exception:
        print("El puerto 8000 de Prometheus ya está en uso. Continuando...")

    # 2. Inicializar el total con el histórico real de la base de datos
    try:
        with sqlite3.connect(DB_PATH) as con:
            total_db = con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
            eventos_total.set(total_db)
            print(f"Métrica de total inicializada con el histórico de la BD: {total_db} eventos.")
    except Exception as e:
        print(f"No se pudo inicializar la métrica total desde la BD: {e}")
        eventos_total.set(0)

    while True:
        try:
            evento = obtener_clima()
            future = producer.send(TOPIC, value=evento)
            guardar(evento)
            future.get(timeout=10)

            # 3. Actualizar métricas de Prometheus
            eventos_total.inc()
            temp_gauge.set(evento['temperatura'])
            humedad_gauge.set(evento['humedad'])
            viento_gauge.set(evento['velocidad_viento'])
            presion_gauge.set(evento['presion'])

            ts = evento["timestamp"][11:19]
            print(
                f"[{ts}] OK  temp={evento['temperatura']}°C  "
                f"humedad={evento['humedad']}%  "
                f"viento={evento['velocidad_viento']}m/s  "
                f"desc={evento['descripcion']}"
            )

        except requests.exceptions.RequestException as e:
            print(f"[ERROR API] {e}")
            errores_envio.inc()
        except Exception as e:
            print(f"[ERROR KAFKA] {e}")
            errores_envio.inc()

        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
