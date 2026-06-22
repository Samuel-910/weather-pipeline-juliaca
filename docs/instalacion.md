# Instalación y Configuración del Entorno

Para levantar y ejecutar el **Weather Pipeline Juliaca**, es necesario preparar tanto la infraestructura base (a través de contenedores) como el entorno local de desarrollo. 

---

## Requisitos de Sistema

Asegúrate de contar con los siguientes elementos instalados en tu máquina:

- **Sistema Operativo**: Windows 10/11 (con soporte de consola CMD o PowerShell) o Linux/macOS.
- **Docker + Docker Compose**: Necesario para levantar el contenedor de Apache Kafka y Zookeeper de forma aislada.
- **Python 3.9+**: Lenguaje base para los scripts del productor, observabilidad, dashboard y machine learning.
- **Java 11 (JDK 11)**: Requisito indispensable para Apache Spark. Spark requiere una versión compatible de Java para poder procesar los Jobs. El proyecto incluye un instalador o carpeta `jdk-11.0.2` para Windows.

---

## Estructura de Variables de Entorno (`.env`)

En la raíz del proyecto se debe configurar un archivo `.env` que contenga los accesos a la API de clima y los parámetros de mensajería de Kafka. 

Ejemplo de contenido para `.env`:

```env
API_KEY=tu_api_key_de_openweathermap
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=weather-events
CIUDAD=Juliaca
PAIS=PE
INTERVALO_SEGUNDOS=5
```

> [!NOTE]
> - `API_KEY`: Se obtiene gratuitamente registrándose en [OpenWeatherMap](https://openweathermap.org/api).
> - `INTERVALO_SEGUNDOS`: Tiempo de espera entre peticiones del productor. Para pruebas en tiempo real, un valor de 5 o 10 segundos es óptimo, mientras que para producción se recomiendan intervalos mayores (ej. 60 segundos o más) para evitar exceder las cuotas de la API.

---

## Configuración Especial para Windows (Hadoop / Spark)

Apache Spark requiere binarios de Hadoop (`winutils.exe`) para interactuar con sistemas de archivos y variables del sistema en entornos Windows.

Para resolver esto sin necesidad de instalaciones complejas a nivel global, el proyecto incluye una carpeta local llamada `hadoop/` en la raíz. Los scripts configuran este parche en tiempo de ejecución:

```python
# Configurar HADOOP_HOME para Spark en Windows
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "hadoop")
os.environ["hadoop.home.dir"] = os.path.join(BASE_DIR, "hadoop")
```

De igual forma, el script por lotes `run_streaming.bat` exporta las variables de entorno necesarias para la terminal antes de invocar a `spark-submit`:

```batch
set "HADOOP_HOME=%~dp0hadoop"
set "hadoop.home.dir=%~dp0hadoop"
set "PATH=%HADOOP_HOME%\bin;%PATH%"
```

---

## Instalación del Entorno Virtual de Python

1. **Crear el entorno virtual**:
   ```powershell
   python -m venv venv
   ```

2. **Activar el entorno virtual**:
   - En Windows (PowerShell):
     ```powershell
     .\venv\Scripts\Activate.ps1
     ```
   - En Windows (CMD):
     ```cmd
     .\venv\Scripts\activate.bat
     ```
   - En Linux/macOS:
     ```bash
     source venv/bin/activate
     ```

3. **Instalar dependencias**:
   Instala las dependencias principales (incluyendo las de documentación y ML):
   ```bash
   pip install -r requirements.txt
   ```
