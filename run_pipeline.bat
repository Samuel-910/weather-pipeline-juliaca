@echo off
title Lanzador del Pipeline Big Data - Juliaca
cls

echo =======================================================
echo    INICIANDO ENTORNO COMPLETO DE BIG DATA & ML
echo =======================================================
echo.

:: Terminal 1: Levantar la infraestructura base (Docker)
echo [1/7] Levantando contenedores Docker (Kafka/Zookeeper)...
start "Terminal 1 - Docker" cmd /k "docker-compose up -d"

:: Pausa de seguridad para asegurar que los puertos de Kafka estén arriba antes de crear el tópico
echo Esperando 10 segundos a que Kafka inicie completamente...
timeout /t 10 /nobreak > nul

:: Terminal 2: Crear el tópico en Kafka
echo [2/7] Creando topico en Kafka...
start "Terminal 2 - Kafka Topic" cmd /k "bash s6_kafka/create_topic.sh"
timeout /t 3 /nobreak > nul

:: Terminal 3: Lanzar Productor de Clima
echo [3/7] Lanzando script Productor (Generador de Eventos)...
start "Terminal 3 - Kafka Producer" cmd /k "call .\venv\Scripts\activate && python s6_kafka/producer.py"
timeout /t 2 /nobreak > nul

:: Terminal 4: Test opcional del Consumidor Kafka (Descomenta la linea de abajo si quieres que se abra siempre)
:: start "Terminal 4 - Kafka Consumer Test" cmd /k "call .\venv\Scripts\activate && python s6_kafka/consumer_test.py"

:: Terminal 5: Lanzar el Job de Spark Streaming con el paquete Kafka
echo [4/7] Sometiendo Job de Spark Streaming (s7_spark)...
start "Terminal 5 - Spark Streaming" cmd /k "call .\venv\Scripts\activate && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 s7_spark/streaming_job.py"
timeout /t 5 /nobreak > nul

:: Terminal 6: Monitor de Observabilidad y Métricas (S8)
echo [5/7] Iniciando monitor de metricas de observabilidad...
start "Terminal 6 - Observabilidad (S8)" cmd /k "call .\venv\Scripts\activate && python s8_observabilidad/metricas.py"
timeout /t 2 /nobreak > nul

:: Terminal 7: Dashboard (Aplicacion Web de visualizacion)
echo [6/7] Lanzando interfaz del Dashboard...
start "Terminal 7 - Dashboard App" cmd /k "call .\venv\Scripts\activate && python dashboard/app.py"
timeout /t 2 /nobreak > nul

:: Terminal 8: Ejecución del Modelo de Machine Learning para Temperatura
echo [7/7] Ejecutando componente de Machine Learning...
start "Terminal 8 - ML Modelo" cmd /k "call .\venv\Scripts\activate && python ml/modelo_temperatura.py"

echo.
echo =======================================================
echo    ¡PROCESO COMPLETADO! Revisa las ventanas abiertas.
echo =======================================================
pause