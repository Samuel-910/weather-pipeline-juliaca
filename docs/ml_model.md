# Modelo de Machine Learning (ML)

El pipeline de clima cuenta con un componente analítico de predicción de temperatura a corto plazo, definido en el módulo [modelo_temperatura.py](file:///d:/Big%20data/unidad%202/weather-pipeline-juliaca/ml/modelo_temperatura.py).

Este script extrae la información climática acumulada históricamente en SQLite, entrena dos algoritmos de regresión supervisada y evalúa su rendimiento para determinar cuál modela mejor las fluctuaciones de temperatura en Juliaca.

---

## 1. Variables de Entrenamiento (Dataset)

Para entrenar y evaluar el modelo, se recuperan los datos estructurados desde la base de datos local:

- **Variables predictoras (Features - `X`)**:
    - `hora_dia`: La hora en la que se tomó la medición (0 a 23), para capturar el ciclo solar diario.
    - `dia_semana`: El día de la semana (0 a 6), para capturar posibles patrones semanales.
    - `humedad`: El porcentaje de humedad relativa.
    - `presion`: La presión atmosférica en hPa.
    - `velocidad_viento`: La velocidad del viento en metros por segundo.
- **Variable a predecir (Target - `y`)**:
    - `temperatura`: Temperatura ambiente real registrada en grados Celsius.

> [!WARNING]
> **Requisito de datos**: El script requiere recolectar un histórico mínimo de **20 eventos** en la base de datos para proceder con el entrenamiento de los algoritmos. De lo contrario, se detendrá y solicitará correr el pipeline por más tiempo.

---

## 2. Modelos de Regresión Implementados

Los datos se dividen aleatoriamente en un 80% para el conjunto de entrenamiento y un 20% para pruebas. Se entrenan dos tipos de regresores:

### Regresión Lineal Múltiple (`LinearRegression`)
Modela una relación lineal directa entre las variables predictoras y la temperatura. 
Debido a la disparidad de escalas entre la presión atmosférica (~630 hPa) y la velocidad del viento (~2 m/s), se aplica **`StandardScaler`** para normalizar los datos a una escala con media 0 y varianza 1 antes de entrenar este regresor.

### Random Forest Regressor (`RandomForestRegressor`)
Un algoritmo no lineal basado en un ensamble de árboles de decisión (configurado localmente con **50 árboles**). Este modelo es excelente para capturar interacciones complejas no lineales entre las variables (por ejemplo, cómo el cruce entre una alta humedad y una hora nocturna reduce bruscamente la sensación de temperatura). No requiere escalado previo de datos.

---

## 3. Métricas de Evaluación

Para comparar el rendimiento de los modelos, se evalúan dos métricas estándar:

- **MAE (Error Absoluto Medio)**: Mide la diferencia promedio absoluta entre las temperaturas predichas y las reales. Un MAE de `1.2°C` indica que, en promedio, las predicciones del modelo se desvían 1.2 grados de la temperatura real.
- **\(R^2\) (Coeficiente de Determinación)**: Representa la proporción de la varianza en la temperatura que es predecible a partir de las features. Valores cercanos a `1.0` indican un ajuste casi perfecto, mientras que valores cercanos o inferiores a `0.0` indican que el modelo no aporta información relevante.

---

## 4. Importancia de las Características (Feature Importances)

El regresor Random Forest calcula automáticamente el nivel de influencia de cada variable predictora al estimar la temperatura. El script imprime esta relevancia visualmente en la terminal utilizando barras de caracteres unicode:

```text
  Importancia de features (Random Forest):
    humedad              ██████████████████████████████ 0.652
    hora_dia             ██████ 0.143
    presion              ████ 0.098
    velocidad_viento     ███ 0.082
    dia_semana           █ 0.025
```

---

## 5. Inferencia en Tiempo Real

Al finalizar la fase de entrenamiento, el script realiza una predicción rápida utilizando la hora real actual de tu sistema como entrada (asumiendo condiciones meteorológicas estándar de Juliaca, ej: presión de 630 hPa y humedad del 70%) para mostrar el comportamiento estimado del clima en ese instante:

```python
hora_actual = datetime.now().hour
ejemplo = np.array([[hora_actual, 0, 70, 630, 2.0]])
pred = rf.predict(ejemplo)[0]
```
