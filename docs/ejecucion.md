# Guía de Ejecución y Despliegue

Esta guía detalla los pasos para iniciar cada uno de los componentes de la arquitectura del **Weather Pipeline Juliaca**. Puedes optar por realizar la ejecución de forma manual (paso a paso en terminales independientes) o utilizar los scripts automatizados proporcionados.

---

## Infraestructura en Contenedores (Docker)

El archivo `docker-compose.yml` en la raíz define los servicios del ecosistema:
- **Zookeeper** (puerto `2181` interno)
- **Apache Kafka** (puerto `9092` expuesto al host)
- **Prometheus** (puerto `9090` para visualizar y evaluar reglas de alertas)
- **Kafka-UI** (puerto `8080` para examinar de manera visual tópicos, offsets y mensajes en cola)
- **Grafana** (puerto `3000` con el plugin de SQLite preinstalado)

Levanta la infraestructura base ejecutando:
```bash
docker-compose up -d
```

---

## Ejecución Paso a Paso

Si deseas controlar la inicialización de forma manual para monitorear las salidas por consola, abre terminales distintas y ejecuta el siguiente orden secuencial:

### Paso 1: Crear Tópico en Kafka
Asegúrate de que Kafka esté arriba y ejecuta el script de inicialización:
```bash
bash s6_kafka/create_topic.sh
```

### Paso 2: Activar Entorno Virtual
En cada nueva terminal de comandos que abras para ejecutar scripts en Python, recuerda activar el entorno virtual correspondiente:
```powershell
# En Windows (PowerShell)
.\venv\Scripts\Activate.ps1
```

### Paso 3: Iniciar el Productor Climático
El productor comenzará a realizar peticiones climáticas y a publicar eventos JSON en Kafka, además de exponer métricas Prometheus en el puerto `8000`:
```bash
python s6_kafka/producer.py
```

### Paso 4: Lanzar el Procesador en Streaming (Spark)
Envía el job a Spark Streaming cargando dinámicamente el paquete de conexión con Kafka:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 s7_spark/streaming_job.py
```

### Paso 5: Iniciar Monitor de Observabilidad
Arranca el script de métricas operativas de canal que computa el lag de offsets y la contrapresión:
```bash
python s8_observabilidad/metricas.py
```

### Paso 6: Arrancar el Dashboard Web
Levanta el servidor Flask de visualización interactiva en tiempo real:
```bash
python dashboard/app.py
```
Abre tu navegador e ingresa a: [http://localhost:5000](http://localhost:5000).

### Paso 7: Modelado de Machine Learning (ML)
Una vez que hayas acumulado un mínimo de 20 a 50 eventos en la base de datos local, puedes entrenar y evaluar los modelos predictivos de temperatura ejecutando:
```bash
python ml/modelo_temperatura.py
```

---

## Automatización en Windows (`.bat`)

Para agilizar el flujo de trabajo en entornos de desarrollo Windows, se proporcionan dos scripts por lotes que configuran variables de entorno e inician terminales en segundo plano de manera automática:

### A. Ejecución Completa (`run_pipeline.bat`)
Abre e inicializa secuencialmente las terminales de comandos CMD para cada uno de los componentes, aplicando retrasos inteligentes para garantizar que los brokers de Kafka y servicios de red estén listos antes de levantar los jobs:
- Levanta Docker Compose.
- Espera 10 segundos y crea el tópico.
- Arranca el Productor de Clima.
- Envía el Job de Spark.
- Inicializa el monitor de observabilidad y métricas.
- Inicia el Dashboard en tiempo real.
- Corre la fase de Machine Learning.

Para usarlo, haz doble clic sobre el archivo o ejecútalo desde tu consola:
```powershell
.\run_pipeline.bat
```

### B. Ejecución de Spark (`run_streaming.bat`)
Configura rápidamente las variables de entorno de Hadoop local (`winutils.exe`) en el `PATH` de la sesión y ejecuta de forma directa el Spark submit para depuración exclusiva del flujo analítico:
```powershell
.\run_streaming.bat
```

---

## Despliegue Continuo en GitHub Pages (CI/CD)

El repositorio incluye un flujo de trabajo automatizado de GitHub Actions configurado en [.github/workflows/deploy.yml](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/.github/workflows/deploy.yml). Cada vez que subas cambios (`git push`) a las ramas principales `main` o `master`, GitHub compilará automáticamente el sitio y lo publicará en GitHub Pages.

### Configuración en GitHub

Para que la automatización funcione correctamente, debes configurar los permisos de escritura del token en tu repositorio en GitHub:

1. Ve a la página de tu repositorio en GitHub.
2. Dirígete a **Settings** (Configuración) > **Actions** > **General**.
3. Baja hasta la sección **Workflow permissions** (Permisos del flujo de trabajo).
4. Selecciona la opción **Read and write permissions** (Permisos de lectura y escritura).
5. Haz clic en **Save** (Guardar).

Una vez configurado, las ejecuciones del workflow tendrán autorización para compilar el sitio y actualizar la rama de publicación `gh-pages` de manera transparente. Puedes monitorear las compilaciones desde la pestaña **Actions** de tu repositorio.
