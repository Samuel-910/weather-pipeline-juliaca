"""
Almacenamiento de eventos para ML
Guarda cada evento de Kafka en:
  - SQLite  → consultas rápidas y análisis
  - CSV     → fácil de abrir en Excel / pandas
  - Parquet → para Spark MLlib (futuro)
"""

import os
import csv
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

# Carpeta donde se guardan los datos
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH      = DATA_DIR / "weather_juliaca.db"
CSV_PATH     = DATA_DIR / "weather_juliaca.csv"
PARQUET_DIR  = DATA_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)

_lock = threading.Lock()


# ─── SQLite ────────────────────────────────────────────────────────────────────

def init_db():
    """Crea la tabla si no existe."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT,
                ciudad           TEXT,
                temperatura      REAL,
                sensacion_termica REAL,
                temp_min         REAL,
                temp_max         REAL,
                humedad          INTEGER,
                presion          INTEGER,
                descripcion      TEXT,
                velocidad_viento REAL,
                visibilidad      INTEGER,
                hora_dia         INTEGER,
                dia_semana       INTEGER,
                fecha_insercion  TEXT DEFAULT (datetime('now'))
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos(timestamp)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_hora ON eventos(hora_dia)
        """)
        con.commit()
    print(f"Base de datos lista: {DB_PATH}")


def guardar_sqlite(evento: dict):
    campos = [
        "timestamp", "ciudad", "temperatura", "sensacion_termica",
        "temp_min", "temp_max", "humedad", "presion", "descripcion",
        "velocidad_viento", "visibilidad", "hora_dia", "dia_semana",
    ]
    valores = tuple(evento.get(c) for c in campos)
    sql = f"""
        INSERT INTO eventos ({', '.join(campos)})
        VALUES ({', '.join(['?'] * len(campos))})
    """
    with _lock:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(sql, valores)
            con.commit()


# ─── CSV ───────────────────────────────────────────────────────────────────────

CAMPOS_CSV = [
    "timestamp", "ciudad", "temperatura", "sensacion_termica",
    "temp_min", "temp_max", "humedad", "presion", "descripcion",
    "velocidad_viento", "visibilidad", "hora_dia", "dia_semana",
]

def init_csv():
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV)
            writer.writeheader()
        print(f"CSV listo: {CSV_PATH}")


def guardar_csv(evento: dict):
    fila = {c: evento.get(c, "") for c in CAMPOS_CSV}
    with _lock:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV)
            writer.writerow(fila)


# ─── Parquet (para Spark MLlib) ────────────────────────────────────────────────

_buffer_parquet = []
_PARQUET_BATCH  = 50   # guardar cada 50 eventos

def guardar_parquet(evento: dict):
    """
    Acumula eventos y guarda en Parquet cada 50.
    Requiere: pip install pyarrow pandas
    """
    try:
        import pandas as pd

        with _lock:
            _buffer_parquet.append({c: evento.get(c) for c in CAMPOS_CSV})
            if len(_buffer_parquet) >= _PARQUET_BATCH:
                df  = pd.DataFrame(_buffer_parquet)
                ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
                out = PARQUET_DIR / f"weather_{ts}.parquet"
                df.to_parquet(out, index=False)
                print(f"Parquet guardado: {out} ({len(_buffer_parquet)} filas)")
                _buffer_parquet.clear()

    except ImportError:
        pass   # pyarrow no instalado, se ignora silenciosamente


# ─── Función principal: guardar en todos los destinos ─────────────────────────

def guardar(evento: dict):
    """Llama esto con cada evento de Kafka."""
    try:
        guardar_sqlite(evento)
    except Exception as e:
        print(f"[ERROR SQLite] {e}")
    try:
        guardar_csv(evento)
    except Exception as e:
        print(f"[ERROR CSV] {e}")
    try:
        guardar_parquet(evento)
    except Exception as e:
        print(f"[ERROR Parquet] {e}")


# ─── Consultas útiles para ML ──────────────────────────────────────────────────

def leer_para_ml():
    """
    Retorna DataFrame listo para entrenar un modelo.
    Features: hora_dia, dia_semana, humedad, presion, velocidad_viento
    Target:   temperatura
    """
    try:
        import pandas as pd
        df = pd.read_sql(
            "SELECT * FROM eventos ORDER BY timestamp",
            sqlite3.connect(DB_PATH),
        )
        print(f"Dataset: {len(df)} filas · {df.shape[1]} columnas")
        print(df[["temperatura","humedad","presion","hora_dia"]].describe())
        return df
    except Exception as e:
        print(f"Error al leer DB: {e}")
        return None


def resumen_db():
    with sqlite3.connect(DB_PATH) as con:
        total = con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
        prim  = con.execute("SELECT MIN(timestamp) FROM eventos").fetchone()[0]
        ult   = con.execute("SELECT MAX(timestamp) FROM eventos").fetchone()[0]
        tprom = con.execute("SELECT AVG(temperatura) FROM eventos").fetchone()[0]
    print(f"\n{'='*50}")
    print(f"  Resumen base de datos — Juliaca")
    print(f"{'='*50}")
    print(f"  Total eventos  : {total}")
    print(f"  Primer evento  : {prim}")
    print(f"  Último evento  : {ult}")
    print(f"  Temp. promedio : {round(tprom,2) if tprom else '—'}°C")
    print(f"  Archivo DB     : {DB_PATH}")
    print(f"  Archivo CSV    : {CSV_PATH}")
    print(f"{'='*50}\n")


# Inicializar al importar
init_db()
init_csv()


if __name__ == "__main__":
    resumen_db()
    df = leer_para_ml()
    if df is not None and len(df) > 10:
        print("\nPrimeras filas del dataset para ML:")
        print(df[["timestamp","temperatura","humedad","presion",
                  "hora_dia","dia_semana"]].head(10).to_string())
