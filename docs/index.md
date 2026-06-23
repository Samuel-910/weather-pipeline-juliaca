# Weather Pipeline Juliaca

Bienvenido a la documentación oficial del **Weather Pipeline Juliaca**, un proyecto de Big Data diseñado para la ingesta, procesamiento en tiempo real, almacenamiento local, modelado predictivo y visualización de datos climatológicos para la ciudad de Juliaca, Perú.

Este pipeline combina tecnologías clave del ecosistema de datos moderno para ofrecer un flujo continuo y robusto de información climática, aplicando técnicas avanzadas de streaming distribuido, observabilidad operativa y aprendizaje automático.

---

## Arquitectura del Sistema

El pipeline se compone de cinco etapas fundamentales que operan de forma sincronizada. A continuación, se detalla el flujo de datos:

```mermaid
graph TD
    subgraph Ingesta["1. Ingesta de Datos S6"]
        OWM["API OpenWeatherMap"] -->|"HTTP requests por minuto"| Prod["Productor Kafka producer.py"]
        Prod -->|"Publica JSON"| Kafka["Kafka Broker weather-events"]
        Prod -->|"Expone métricas"| Prom1["Prometheus Port 8000"]
    end

    subgraph Procesamiento["2. Procesamiento y Agregacion S7"]
        Kafka -->|"Consumo en Streaming"| Spark["Spark Streaming streaming_job.py"]
        Spark -->|"Ventanas de 5 min y Watermark 2 min"| console["Consola Spark Update Mode"]
    end

    subgraph Almacenamiento["3. Almacenamiento Local SQLite-CSV-Parquet"]
        Prod -->|"Mutex Lock y Guardar"| DB["SQLite weather_juliaca.db"]
        Prod -->|"Mutex Lock y Guardar"| CSV["CSV weather_juliaca.csv"]
        Prod -->|"Mutex Lock y Lotes 50"| Parquet["Parquet data-parquet"]
    end

    subgraph ConsumoML["4. Modelado y Visualizacion"]
        DB -->|"Pandas read_sql"| ML["Machine Learning modelo_temperatura.py"]
        ML -->|"Random Forest y Gradient Boosting"| Pred["Predicción de Temperatura"]
        
        Kafka -->|"Consumo en tiempo real"| Dash["Flask Web App app.py"]
        Dash -->|"Server-Sent Events SSE"| Web["Navegador Dashboard Chart.js"]
    end

    subgraph Observabilidad["5. Monitoreo S8"]
        Metric["Métricas de Canal metricas.py"]
        Metric -->|"Calcula Lag y Backpressure"| Kafka
        Metric -->|"Escribe logs"| Logs["Logs de Operación pipeline-log"]
        Metric -->|"Alertas en consola"| Terminal["Terminal de Monitoreo"]
    end

    style OWM fill:#e0f2fe,stroke:#0284c7,stroke-width:2px
    style Kafka fill:#fee2e2,stroke:#dc2626,stroke-width:2px
    style Spark fill:#ffedd5,stroke:#ea580c,stroke-width:2px
    style DB fill:#ecfdf5,stroke:#059669,stroke-width:2px
    style CSV fill:#ecfdf5,stroke:#059669,stroke-width:2px
    style Parquet fill:#ecfdf5,stroke:#059669,stroke-width:2px
    style ML fill:#faf5ff,stroke:#7c3aed,stroke-width:2px
    style Dash fill:#f0fdf4,stroke:#16a34a,stroke-width:2px
    style Web fill:#f0fdf4,stroke:#16a34a,stroke-width:2px
    style Metric fill:#fef3c7,stroke:#d97706,stroke-width:2px
```

---

## Componentes Principales

1. **Ingesta de Datos (S6)**: Un productor desarrollado en Python que realiza peticiones periódicas a la API de OpenWeatherMap, publica datos en Apache Kafka y genera métricas expuestas para Prometheus.
2. **Procesamiento en Tiempo Real (S7)**: Un Job de Spark Structured Streaming que lee los datos climáticos en tiempo real, aplica agregaciones basadas en ventanas de tiempo y gestiona eventos retrasados a través de técnicas de *Watermarking*.
3. **Almacenamiento Multicanal**: Un módulo robusto encargado de persistir los datos de manera concurrente en SQLite para consultas ágiles, en CSV para compatibilidad, y en formato columnar Parquet optimizado para análisis futuros.
4. **Dashboard Interactivo**: Aplicación web construida con Flask y Server-Sent Events (SSE) para renderizar gráficos dinámicos en tiempo real sobre la temperatura, humedad, presión atmosférica y mapa de calor de 24 horas usando Chart.js.
5. **Machine Learning Predictivo**: Script que extrae los datos históricos de SQLite y entrena algoritmos de Random Forest y Gradient Boosting para predecir la temperatura de Juliaca a partir de factores ambientales.
6. **Observabilidad Operativa (S8)**: Sistema continuo que monitoriza la latencia del pipeline, la tasa de errores y calcula la contrapresión (*backpressure*) en base al lag del offset en Kafka.

---

## Objetivos del Proyecto

- **Procesamiento de Streaming de Extremo a Extremo**: Construir e integrar un pipeline funcional que conecte fuentes externas (APIs), mensajería en tiempo real (Kafka), procesamiento distribuido (Spark) y consumo analítico.
- **Toma de Decisiones Predictivas**: Evaluar variables ambientales que influyen en el comportamiento térmico de la ciudad altoandina de Juliaca.
- **Aseguramiento Operativo**: Demostrar el uso de métricas e indicadores de rendimiento operativo para la detección temprana de fallos o cuellos de botella en la ingesta.
