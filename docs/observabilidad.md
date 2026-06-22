# Observabilidad y Monitoreo (S8)

El componente **S8** del pipeline proporciona visibilidad en tiempo real sobre el estado operacional del sistema. El monitoreo es vital en arquitecturas de Big Data para identificar cuellos de botella (como retrasos en el procesamiento o caídas de APIs) antes de que impacten a los consumidores finales.

El sistema de observabilidad del proyecto se divide en dos enfoques:
1. **Métricas y Alertas Locales** (implementadas en Python a través de `metricas.py`).
2. **Monitoreo Distribuido** (con soporte para Prometheus y reglas de alerta).

---

## 1. Métricas Locales y Umbrales Operativos (`metricas.py`)

El script [metricas.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/s8_observabilidad/metricas.py) lee de forma continua el comportamiento de la cola e implementa los siguientes umbrales operativos bajo la estructura `UMBRALES`:

- `latencia_max_s` (5.0 segundos): Tiempo máximo tolerado para procesar un lote individual.
- `error_rate_max_pct` (5.0%): Tasa máxima permitida de fallos de ingesta.
- `backpressure_max_pct` (80.0%): Nivel crítico de acumulación de mensajes en Kafka.
- `throughput_min_ev_min` (0.5 eventos/minuto): Rendimiento mínimo esperado en la ingesta.

### Cálculo de Contrapresión (Backpressure)
La contrapresión ocurre cuando el motor de procesamiento (Spark) es más lento que la fuente de datos (Kafka), causando una acumulación de mensajes en la cola. El script calcula esto midiendo el **Lag de Kafka**:

```python
particiones = consumer.partitions_for_topic(TOPIC) or {0}
lag_total = 0
for p in particiones:
    tp = TopicPartition(TOPIC, p)
    end_offsets    = consumer.end_offsets([tp])
    current_offset = consumer.position(tp)
    lag_total += end_offsets[tp] - current_offset
```

- **`end_offsets`**: El último offset de mensaje escrito en Kafka.
- **`current_offset`**: La posición de lectura del grupo de Spark.
- La diferencia determina el número de mensajes en cola de espera. Si la proporción supera el 80% respecto a la escala histórica, se dispara una alerta de `backpressure` en los logs.

---

## 2. Sistema de Alertas y Logging Diario

El monitor escribe todas las métricas operativas y las alertas directamente en consola y en un archivo de log rotativo diario almacenado en la carpeta `s8_observabilidad/logs/`:
- Formato del log: `pipeline_YYYY-MM-DD.log`.
- Se registran alertas de tipo `WARNING` si la latencia sube o el rendimiento baja, y `ERROR` si la tasa de errores o la contrapresión superan los límites.

### Resumen Operativo
Al detener el script manualmente presionando `Ctrl + C`, se captura la señal `KeyboardInterrupt` para procesar y presentar un reporte acumulativo del rendimiento total observado:

```text
============================================================
RESUMEN OPERATIVO DEL PIPELINE
============================================================
  Batches procesados : 45
  Total eventos      : 450
  Total errores      : 1
  Latencia promedio  : 2.14s
  Log guardado en    : s8_observabilidad/logs/pipeline_2026-06-22.log
============================================================
```

---

## 3. Integración con Prometheus y Grafana

Para proyectos distribuidos de gran escala, las alertas en consola son insuficientes. El proyecto incluye archivos listos para levantar un stack de observabilidad moderno:

### Configuración de Prometheus (`prometheus.yml`)
Configura a Prometheus para recopilar (`scrape`) las métricas del puerto **8000** del Productor cada 5 segundos:

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'weather-producer'
    static_configs:
      - targets: ['host.docker.internal:8000']
```

### Reglas de Alertas en Prometheus (`prometheus_rules.yml`)
Define disparadores automatizados basados en el estado del pipeline:

1. **`ProducerDown` (Severidad: Critical)**: Se activa si el productor deja de responder a Prometheus por más de 1 minuto (`up == 0`).
2. **`HighApiErrors` (Severidad: Warning)**: Se activa si se registran errores continuos de red o de comunicación hacia Kafka durante más de 1 minuto.
3. **`ExtremeTemperature` (Severidad: Warning)**: Alerta orientada a negocio. Se activa si la temperatura de Juliaca se sale del rango habitual de montaña (-10°C a 30°C) por más de 5 minutos continuos.
