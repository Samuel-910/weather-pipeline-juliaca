#!/bin/bash
# S6 — Crear tópico weather-events en Kafka
# Ejecutar después de: docker-compose up -d

TOPIC="weather-events"
BROKER="localhost:9092"
PARTICIONES=1
REPLICACION=1

echo "Creando tópico: $TOPIC"

docker exec kafka kafka-topics \
  --create \
  --topic $TOPIC \
  --bootstrap-server $BROKER \
  --partitions $PARTICIONES \
  --replication-factor $REPLICACION \
  --if-not-exists

echo ""
echo "Verificando tópico:"
docker exec kafka kafka-topics \
  --describe \
  --topic $TOPIC \
  --bootstrap-server $BROKER
