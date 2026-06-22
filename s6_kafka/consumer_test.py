"""
S6 — Consumer de prueba
Verifica que los mensajes llegan correctamente al tópico.
Ejecutar en terminal separada mientras corre el producer.
"""

import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "weather-events")

print(f"Escuchando tópico: {TOPIC}")
print("-" * 60)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=300_000,
)

for msg in consumer:
    evento = msg.value
    print(f"\nPartición: {msg.partition} | Offset: {msg.offset}")
    print(json.dumps(evento, indent=2, ensure_ascii=False))
    print("-" * 60)
