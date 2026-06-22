"""
ML básico — Predicción de temperatura en Juliaca
Lee datos guardados en SQLite y entrena un modelo de regresión.
Ejecutar después de acumular al menos 50 eventos.

Uso: python ml/modelo_temperatura.py
"""

import os
import sys
import sqlite3
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'weather_juliaca.db')


def cargar_datos():
    try:
        import pandas as pd
        df = pd.read_sql(
            """
            SELECT timestamp, temperatura
            FROM eventos
            WHERE temperatura IS NOT NULL
            ORDER BY timestamp
            """,
            sqlite3.connect(DB_PATH),
        )
        print(f"Datos cargados: {len(df)} eventos")
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None


def entrenar_modelo(df):
    try:
        import pandas as pd
        from sklearn.linear_model import LinearRegression
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, r2_score
        from sklearn.preprocessing import StandardScaler

        # Parsear variables temporales a partir de timestamp
        df['dt'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('America/Lima')
        df['hora_dia'] = df['dt'].dt.hour
        df['dia_semana'] = df['dt'].dt.weekday
        df['mes'] = df['dt'].dt.month
        df['semana_anio'] = df['dt'].dt.isocalendar().week.astype(int)

        # Features y target
        features = ["hora_dia", "dia_semana", "mes", "semana_anio"]
        X = df[features].values
        y = df["temperatura"].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_test_s  = scaler.transform(X_test)

        print("\n" + "="*50)
        print("  Entrenamiento de modelos — Juliaca")
        print("="*50)

        # Regresión lineal
        lr = LinearRegression()
        lr.fit(X_train_s, y_train)
        pred_lr = lr.predict(X_test_s)
        mae_lr  = mean_absolute_error(y_test, pred_lr)
        r2_lr   = r2_score(y_test, pred_lr)
        print(f"\n  Regresión Lineal:")
        print(f"    MAE = {mae_lr:.2f}°C")
        print(f"    R²  = {r2_lr:.3f}")

        # Random Forest (Optimizado con n_jobs=-1 para entrenamiento paralelo)
        rf = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)
        pred_rf = rf.predict(X_test)
        mae_rf  = mean_absolute_error(y_test, pred_rf)
        r2_rf   = r2_score(y_test, pred_rf)
        print(f"\n  Random Forest:")
        print(f"    MAE = {mae_rf:.2f}°C")
        print(f"    R²  = {r2_rf:.3f}")

        # Importancia de features
        print(f"\n  Importancia de features (Random Forest):")
        for f, imp in sorted(
            zip(features, rf.feature_importances_),
            key=lambda x: -x[1]
        ):
            bar = "█" * int(imp * 30)
            print(f"    {f:<20} {bar} {imp:.3f}")

        # Predicción de ejemplo con fecha y hora actuales
        import datetime
        ahora = datetime.datetime.now()
        hora_actual = ahora.hour
        dia_semana_actual = ahora.weekday()
        mes_actual = ahora.month
        semana_actual = ahora.isocalendar()[1]

        ejemplo = np.array([[hora_actual, dia_semana_actual, mes_actual, semana_actual]])
        pred = rf.predict(ejemplo)[0]
        print(f"\n  Predicción ahora ({hora_actual:02d}:00h, día_semana={dia_semana_actual}, mes={mes_actual}, sem={semana_actual}):")
        print(f"    Temperatura estimada: {pred:.1f}°C")
        print("="*50)

        return rf, scaler

    except ImportError:
        print("Instalar: pip install scikit-learn pandas")
        return None, None


def main():
    df = cargar_datos()
    if df is None or len(df) < 20:
        print(f"Necesitas al menos 20 eventos. Tienes: {len(df) if df is not None else 0}")
        print("Deja correr el pipeline más tiempo y vuelve a ejecutar este script.")
        return

    entrenar_modelo(df)


if __name__ == "__main__":
    main()
