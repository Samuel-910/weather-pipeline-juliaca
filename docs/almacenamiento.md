# Almacenamiento de Datos (SQLite/CSV/Parquet)

El sistema de persistencia de datos climáticos está centralizado en el módulo [almacenamiento.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/data/almacenamiento.py). Su función es almacenar de manera eficiente cada evento recibido desde la API para que esté disponible para análisis históricos, paneles web y entrenamiento de modelos de Machine Learning.

---

## 1. Persistencia Multicanal

Para dar soporte a diferentes cargas de trabajo y necesidades analíticas, cada evento del clima se escribe de forma paralela en tres destinos distintos:

### A. SQLite (`weather_juliaca.db`)
Se utiliza para realizar consultas estructuradas en SQL rápidas.
Al inicializarse, crea una tabla llamada `eventos` con índices optimizados en los campos de tiempo e historial horario para maximizar la velocidad de lectura en el Dashboard y en los modelos de Machine Learning:
- Índice `idx_timestamp` sobre el campo `timestamp`.
- Índice `idx_hora` sobre el campo `hora_dia`.

### B. CSV (`weather_juliaca.csv`)
Un archivo de texto plano delimitado por comas, fácil de abrir en herramientas ofimáticas tradicionales (como Microsoft Excel) o scripts sencillos con la librería Pandas en Python. 
Si el archivo CSV no existe en el disco, se genera automáticamente escribiendo la cabecera con el orden específico de columnas.

### C. Parquet (`data/parquet/`)
Formato columnar y comprimido de almacenamiento que permite realizar lecturas de alto rendimiento en herramientas analíticas de Big Data como Apache Spark o Amazon Athena. 

Para evitar la creación excesiva de micro-archivos Parquet en el disco (un problema común en sistemas de archivos distribuidos), el script implementa un **Buffer de acumulación**:
- Guarda los eventos en una lista interna en memoria (`_buffer_parquet`).
- Al alcanzar un lote (`batch`) de **50 eventos**, convierte la lista en un DataFrame de Pandas y la exporta como un archivo Parquet único con la marca de tiempo correspondiente (`weather_YYYYMMDD_HHMMSS.parquet`).
- Limpia el buffer y reinicia el contador.

---

## 2. Gestión de Concurrencia (Mutex Lock)

Dado que la ingesta ocurre en hilos asíncronos y puede haber peticiones concurrentes escribiendo sobre los mismos archivos, se utiliza un bloqueo de exclusión mutua (**Mutex Lock** de la librería nativa `threading`):

```python
_lock = threading.Lock()
```

Cada una de las funciones de escritura (`guardar_sqlite`, `guardar_csv`, `guardar_parquet`) envuelve su lógica crítica bajo el bloque:

```python
with _lock:
    # Lógica de inserción o escritura en archivo
```

Esto garantiza la integridad referencial y evita la corrupción de archivos o bloqueos de escritura simultáneos en SQLite.

---

## 3. Funciones Auxiliares para Machine Learning y Operaciones

El módulo proporciona herramientas integradas para interactuar con la base de datos:

- **`leer_para_ml()`**: Consulta todos los registros almacenados en la tabla SQLite ordenados cronológicamente y los retorna estructurados en un DataFrame de Pandas, listo para el entrenamiento de modelos predictivos.
- **`resumen_db()`**: Ejecuta funciones de agregación en SQL (`COUNT`, `MIN`, `MAX`, `AVG`) e imprime un reporte completo en la consola de comandos con el total de eventos, la fecha del primer y último evento y la temperatura media registrada.
