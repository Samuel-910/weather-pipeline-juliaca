# Weather Pipeline Juliaca
Pipeline streaming Kafka + Spark para datos climáticos de Juliaca, PE.

## Requisitos
- Docker + Docker Compose
- Python 3.9+
- Java 11 (requerido por Spark)

## Instalación

```bash
pip install -r requirements.txt
```

## Ejecución paso a paso

### 1. Levantar Kafka
```bash
docker-compose up -d
```

### 2. Crear tópico (S6)
```bash
bash s6_kafka/create_topic.sh
```

### 3. Iniciar productor (S6) — terminal 1
```bash
python s6_kafka/producer.py
```

### 4. Verificar mensajes (S6) — terminal 2
```bash
python s6_kafka/consumer_test.py
```

### 5. Iniciar Spark Streaming (S7) — terminal 3
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  s7_spark/streaming_job.py
```

### 6. Pruebas de parámetros (S7) — terminal 4
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  s7_spark/test_parametros.py
```

### 7. Monitor de métricas (S8) — terminal 5
```bash
python s8_observabilidad/metricas.py
```

## Estructura
```
weather-pipeline-juliaca/
├── .env                        ← API key y configuración
├── docker-compose.yml          ← Kafka + Zookeeper
├── requirements.txt
├── s6_kafka/
│   ├── create_topic.sh
│   ├── producer.py
│   ├── consumer_test.py
│   └── evidencias/
├── s7_spark/
│   ├── streaming_job.py        ← Job principal
│   ├── test_parametros.py      ← Pruebas S7
│   ├── checkpoint/             ← Auto-generado por Spark
│   └── evidencias/
└── s8_observabilidad/
    ├── metricas.py
    └── logs/                   ← Auto-generado
```
"# weather-pipeline-juliaca" 
