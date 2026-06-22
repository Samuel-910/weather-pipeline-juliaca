# Componente de Ingesta: Apache Kafka (S6)

El componente **S6** gestiona el ciclo inicial de ingesta de datos climáticos. Utiliza **Apache Kafka** como un sistema de colas distribuido y tolerante a fallos para desacoplar el productor de datos climáticos de los múltiples consumidores (Spark Streaming, Dashboard, etc.).

---

## 1. Creación del Tópico Kafka

El script [create_topic.sh](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/s6_kafka/create_topic.sh) crea el tópico `weather-events` dentro del broker de Kafka ejecutándose bajo el contenedor de Docker.

```bash
#!/bin/bash
TOPIC="weather-events"
BROKER="localhost:9092"
PARTICIONES=1
REPLICACION=1

docker exec kafka kafka-topics \
  --create \
  --topic $TOPIC \
  --bootstrap-server $BROKER \
  --partitions $PARTICIONES \
  --replication-factor $REPLICACION \
  --if-not-exists
```

Este script:
- Se conecta al contenedor `kafka` usando `docker exec`.
- Configura 1 partición y un factor de replicación de 1 (adecuado para entornos de desarrollo local).
- Comprueba si el tópico existe antes de crearlo (`--if-not-exists`) y finalmente muestra su descripción (`--describe`).

---

## 2. Productor de Datos Climáticos (`producer.py`)

El script [producer.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/s6_kafka/producer.py) es el motor que alimenta el pipeline. Trabaja continuamente de la siguiente forma:

1. **Consulta HTTP**: Cada intervalo de tiempo configurado (por ejemplo, cada 5 segundos), consulta la API de OpenWeatherMap solicitando las condiciones meteorológicas en tiempo real de Juliaca, Perú.
2. **Estructura del Evento**: Transforma la respuesta cruda de la API en un objeto JSON plano con la siguiente estructura:
    - Métricas: `temperatura`, `sensacion_termica`, `temp_min`, `temp_max`, `humedad`, `presion`, `velocidad_viento`, `visibilidad`.
    - Geometría: `latitud`, `longitud`.
    - Metadatos: `timestamp` formateado (ISO 8601), `ciudad`, `pais`, `descripcion` del cielo, `hora_dia` y `dia_semana`.
3. **Publicación y Persistencia**:
    - Envía el JSON codificado en UTF-8 al tópico de Kafka.
    - Persiste el evento localmente llamando a la función `guardar(evento)` (definida en `data/almacenamiento.py`).
4. **Métricas en Tiempo Real (Prometheus)**:
    Inicia un servidor HTTP de Prometheus en el puerto **8000** y actualiza continuamente métricas claves:
    - `weather_events_total`: Número acumulativo de eventos guardados históricamente.
    - `weather_events_errors_total`: Contador de fallos en llamadas HTTP o envíos a Kafka.
    - Indicadores instantáneos (`Gauge`) para `weather_temperature_celsius`, `weather_humidity_percent`, `weather_wind_speed_mps` y `weather_pressure_hpa`.

> [!TIP]
> Al iniciar el productor, este consulta la base de datos SQLite histórica para precargar y sincronizar la métrica `weather_events_total` con el número real de registros guardados anteriormente.

---

## 3. Consumidor de Prueba (`consumer_test.py`)

Para depuración y verificación manual de que los mensajes llegan con éxito a Kafka, se proporciona [consumer_test.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/s6_kafka/consumer_test.py).

Este consumidor:
- Escucha de forma continua el tópico `weather-events`.
- No pertenece a ningún grupo de consumo persistente (`group_id=None` implícito), consumiendo los mensajes más nuevos (`auto_offset_reset="latest"`).
- Imprime por consola los metadatos de partición y offset, junto con el JSON completo formateado.

```bash
python s6_kafka/consumer_test.py
```
