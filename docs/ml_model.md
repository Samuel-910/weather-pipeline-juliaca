# Modelo de Machine Learning (ML)

El pipeline de clima cuenta con un componente analítico de predicción de temperatura a corto plazo, definido en el módulo [modelo_temperatura.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/ml/modelo_temperatura.py).

Este script extrae la información climática acumulada históricamente en SQLite, entrena dos algoritmos de regresión supervisada y evalúa su rendimiento para determinar cuál modela mejor las fluctuaciones de temperatura en Juliaca.

---

## 1. Variables de Entrenamiento (Dataset)

Para entrenar y evaluar el modelo, se recuperan los datos estructurados desde la base de datos local:

- **Variables predictoras (Features - `X`)**:
    - `hora_dia`: La hora en la que se tomó la medición (0 a 23), para capturar el ciclo solar diario de calentamiento y enfriamiento.
    - `dia_semana`: El día de la semana (0 a 6, donde 0 es lunes y 6 es domingo).
    - `mes`: El mes del año (1 a 12), para capturar patrones estacionales.
    - `semana_anio`: La semana ISO del año (1 a 53), para una mayor resolución estacional.
- **Variable a predecir (Target - `y`)**:
    - `temperatura`: Temperatura ambiente real registrada en grados Celsius.

> [!WARNING]
> **Requisito de datos**: El script requiere recolectar un histórico mínimo de **20 eventos** en la base de datos para proceder con el entrenamiento de los algoritmos. De lo contrario, se detendrá y solicitará correr el pipeline por más tiempo.

---

## 2. Modelos de Regresión Implementados

Los datos se dividen aleatoriamente en un 80% para el conjunto de entrenamiento y un 20% para pruebas. Se entrenan dos tipos de regresores:

### Regresión Lineal Múltiple (`LinearRegression`)
Modela una relación lineal directa entre las variables temporales y la temperatura. 
Debido a la disparidad de escalas entre la hora del día (0-23), el día de la semana (0-6), el mes (1-12) y la semana del año (1-53), se aplica **`StandardScaler`** para normalizar los datos a una escala con media 0 y varianza 1 antes de entrenar este regresor.

### Random Forest Regressor (`RandomForestRegressor`)
Un algoritmo no lineal basado en un ensamble de árboles de decisión (configurado localmente con **50 árboles**). Este modelo es excelente para capturar interacciones complejas no lineales entre las variables temporales (por ejemplo, cómo influye la hora de la noche combinada con la estación/mes del año en el descenso de temperatura). No requiere escalado previo de datos.

---

## 3. Métricas de Evaluación

Para comparar el rendimiento de los modelos, se evalúan dos métricas estándar:

- **MAE (Error Absoluto Medio)**: Mide la diferencia promedio absoluta entre las temperaturas predichas y las reales. Un MAE de `0.46°C` indica que, en promedio, las predicciones del modelo se desvían menos de medio grado de la temperatura real.
- **\(R^2\) (Coeficiente de Determinación)**: Representa la proporción de la varianza en la temperatura que es predecible a partir de las features. Valores cercanos a `1.0` indican un ajuste casi perfecto (ej. `0.95`).

---

## 4. Importancia de las Características (Feature Importances)

El regresor Random Forest calcula automáticamente el nivel de influencia de cada variable temporal al estimar la temperatura. El script imprime esta relevancia visualmente en la terminal:

```text
  Importancia de features (Random Forest):
    hora_dia             ████████████████████████ 0.818
    semana_anio          ████ 0.136
    dia_semana           █ 0.046
    mes                   0.000
```

---

## 5. Inferencia en Tiempo Real

Al finalizar la fase de entrenamiento, el script realiza una predicción rápida utilizando los valores temporales del sistema en el instante de la ejecución (hora, día de la semana, mes, y semana del año) para mostrar el comportamiento estimado del clima en Juliaca:

```python
import datetime
ahora = datetime.datetime.now()
hora_actual = ahora.hour
dia_semana_actual = ahora.weekday()
mes_actual = ahora.month
semana_actual = ahora.isocalendar()[1]

ejemplo = np.array([[hora_actual, dia_semana_actual, mes_actual, semana_actual]])
pred = rf.predict(ejemplo)[0]
```
