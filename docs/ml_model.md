# Modelado de Machine Learning (Juliaca)

El pipeline de clima de Juliaca cuenta con un componente analítico de predicción y modelado, el cual fue desarrollado exhaustivamente en el entorno interactivo `ml/files/weather_ml_juliaca.ipynb` y posteriormente integrado en el dashboard en tiempo real (`dashboard/app.py`).

Este documento detalla el análisis de datos reales, la ingeniería de características (Feature Engineering) y la evaluación comparativa de algoritmos de regresión.

---

## 1. Descarga de Datos Reales (API Open-Meteo)

Para entrenar modelos que reflejen la realidad climatológica de Juliaca, se reemplazaron los datos simulados por datos históricos reales extraídos de la API de Open-Meteo (usando el modelo ERA5). 

- Se obtuvieron datos horarios desde enero hasta mayo.
- Se identificaron y rellenaron valores nulos utilizando **interpolación temporal** (`df.interpolate(method='time')`), garantizando que la serie de tiempo mantuviera su continuidad sin saltos abruptos.
- Los datos se guardaron en `juliaca_horario_real.csv` (~2100 registros), el cual es la fuente de verdad actual tanto para el notebook de experimentación como para el dashboard de monitoreo histórico.

---

## 2. Ingeniería de Características (Feature Engineering)

La temperatura es un fenómeno altamente dependiente del tiempo. En el notebook de exploración (`weather_ml_juliaca.ipynb` y `data_utils.py`), en lugar de usar características estáticas simples, se desarrollaron variables predictoras avanzadas basadas en la estructura secuencial de los datos:

1. **Variables Cíclicas:** Se transformó la hora del día en componentes circulares usando el Seno y el Coseno (`hora_sin`, `hora_cos`), ayudando a los modelos lineales a entender que las 23:00 está cerca de las 00:00.
2. **Rezagos (Lags):** Se incluyeron las temperaturas pasadas de hace 1 hora, 2 horas, 24 horas (el día anterior) y 168 horas (la semana anterior). 
3. **Medias Móviles (Rolling Means):** Se calcularon los promedios móviles de las últimas 3 y 24 horas para capturar tendencias de calentamiento o enfriamiento en el corto plazo.

> [!NOTE]
> **Integración en el Dashboard:** 
> Aunque el notebook utiliza *lags* para alcanzar una precisión extrema, el **Dashboard en tiempo real** utiliza un conjunto reducido de variables temporales (`hora_dia`, `dia_semana`, `mes`, `semana_anio`). Esto se diseñó intencionalmente para permitir a los usuarios "simular" predicciones en el futuro (ej. predecir el clima para el próximo año), donde los *lags* de hace 168 horas aún no existen.

---

## 3. Comparación de Algoritmos por Horizonte

Se entrenaron y evaluaron **4 modelos de Machine Learning** distintos para predecir a 3 horizontes de tiempo (1h, 24h, 168h):

1. **Ridge Regression:** Modelo lineal regularizado. Rápido, pero asume relaciones lineales simples.
2. **Random Forest:** Ensamble de 50 árboles de decisión, excelente para capturar relaciones no lineales y patrones cíclicos.
3. **HistGradientBoosting:** Modelo secuencial de boosting hiper-optimizado para grandes volúmenes de datos.
4. **K-Nearest Neighbors (KNN):** Predice promediando la temperatura de los registros pasados que más se parecen a las condiciones actuales.

### Resultados de la Evaluación (Error RMSE)

Tras evaluar el error predictivo, la función `comparar_modelos.py` determinó a los siguientes ganadores:

| Horizonte | Modelo Ganador | MAE Promedio | RMSE Promedio | R² (Ajuste) |
| :--- | :--- | :--- | :--- | :--- |
| **Próxima hora (1h)** | **Random Forest** | `0.64 °C` | `0.91 °C` | `0.96` |
| **Próximo día (24h)** | **Ridge** | `1.06 °C` | `1.38 °C` | `0.91` |
| **Una semana (168h)** | **Random Forest** | `1.06 °C` | `1.35 °C` | `0.91` |

> [!TIP]
> **Conclusión del Modelado:** 
> **Random Forest** domina abrumadoramente en el pronóstico inmediato (1h) al adaptarse rápidamente a pequeños cambios, y también gana en el largo plazo (168h) por su capacidad de entender las estaciones. **Ridge**, gracias a su naturaleza lineal conservadora, logra vencer ligeramente a 24h al evitar el "sobreajuste" (overfitting) a los pequeños ruidos diarios.

---

## 4. Integración Dinámica en la Aplicación Web

En base a estos hallazgos, el dashboard (`dashboard/app.py`) fue actualizado para incluir los 4 algoritmos simultáneamente.

1. Al iniciar o solicitar un reentrenamiento, el backend lee el dataset real (`juliaca_horario_real.csv`).
2. Entrena los 4 modelos y calcula su Error Medio Absoluto (MAE).
3. Selecciona automáticamente el modelo con el menor error, asignándole el título de **"Mejor Modelo"**.
4. Expone las predicciones a través del endpoint `/api/predict-trend`, permitiendo a la interfaz gráfica trazar la curva horaria del Mejor Modelo, comparándola además con el rendimiento del resto de algoritmos.
