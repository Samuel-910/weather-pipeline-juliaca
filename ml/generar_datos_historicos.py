import os
import shutil
import sqlite3
import math
import random
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "weather_juliaca.db")

def respaldar_db():
    if os.path.exists(DB_PATH):
        bak_path = DB_PATH + ".bak"
        print(f"Respaldando base de datos existente en: {bak_path}")
        shutil.copy2(DB_PATH, bak_path)
    else:
        print("No se encontró base de datos previa. Se creará una nueva.")

def inicializar_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    # Limpiar tabla anterior si existiera para asegurar continuidad de datos limpia
    con.execute("DROP TABLE IF EXISTS eventos")
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
    con.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos(timestamp)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_hora ON eventos(hora_dia)")
    con.commit()
    con.close()

def generar_datos():
    print("Iniciando generación de 4 meses de clima para Juliaca (intervalo de 5s)...")
    
    # Rango de tiempo: 120 días
    ahora_local = datetime.now().astimezone() # -05:00
    
    end_time = ahora_local
    start_time = end_time - timedelta(days=120)
    
    # 5 segundos por registro
    step = timedelta(seconds=5)
    
    total_registros = int((end_time - start_time).total_seconds() / 5)
    print(f"Total de registros a generar: {total_registros}")
    
    # Lista de posibles descripciones del clima
    descripciones_lluvia = ["lluvia ligera", "lluvia moderada", "llovizna"]
    descripciones_nubes = ["nubes dispersas", "algo de nubes", "muy nuboso"]
    descripciones_despejado = ["cielo claro", "despejado"]
    
    records = []
    current_time = start_time
    
    con = sqlite3.connect(DB_PATH)
    # Optimizar escritura en SQLite
    con.execute("PRAGMA journal_mode = OFF")
    con.execute("PRAGMA synchronous = OFF")
    con.execute("PRAGMA cache_size = 100000")
    
    sql = """
        INSERT INTO eventos (
            timestamp, ciudad, temperatura, sensacion_termica,
            temp_min, temp_max, humedad, presion, descripcion,
            velocidad_viento, visibilidad, hora_dia, dia_semana
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    batch_size = 100000
    count = 0
    
    while current_time <= end_time:
        dt = current_time
        # Variables temporales para simular
        day_of_year = dt.timetuple().tm_yday
        hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
        
        # Simulación de ciclo térmico y estacional
        # Juliaca solsticio invierno = día 172. En invierno las noches son gélidas y los días soleados.
        # En verano (febrero), el clima es más templado pero más lluvioso.
        # Promedio base estacional de temperatura
        temp_base = 8.5
        temp_estacional = 2.5 * math.cos(2 * math.pi * (day_of_year - 172) / 365.0)
        temp_diaria = 7.5 * math.sin(2 * math.pi * (hour - 9.0) / 24.0)
        ruido = random.normalvariate(0, 0.7)
        
        temperatura = round(temp_base + temp_estacional + temp_diaria + ruido, 2)
        
        # Humedad inversamente proporcional a la temperatura
        humedad_base = 45.0
        humedad_diaria = -25.0 * math.sin(2 * math.pi * (hour - 9.0) / 24.0)
        # Si es época de lluvias (febrero-marzo, day_of_year < 90 o > 330), la humedad es más alta
        es_lluvia = 1.0 if (day_of_year < 90 or day_of_year > 330) else 0.0
        humedad_estacional = 15.0 * es_lluvia
        humedad_ruido = random.normalvariate(0, 4.0)
        
        humedad = int(min(max(humedad_base + humedad_diaria + humedad_estacional + humedad_ruido, 10), 95))
        
        # Presión típica ~1017 hPa con fluctuación de marea atmosférica
        presion_base = 1017.5
        presion_diaria = 1.8 * math.cos(2 * math.pi * hour / 12.0)
        presion_ruido = random.normalvariate(0, 0.4)
        presion = int(presion_base + presion_diaria + presion_ruido)
        
        # Viento brisa de tarde
        viento_base = 1.2
        viento_diario = 2.5 * max(0.0, math.sin(2 * math.pi * (hour - 10.0) / 24.0))
        viento_ruido = random.normalvariate(0, 0.4)
        viento = round(max(0.1, viento_base + viento_diario + viento_ruido), 2)
        
        # Sensación térmica
        sensacion = temperatura + 0.08 * viento - 0.04 * humedad
        
        # Descripciones climáticas realistas
        if es_lluvia > 0.5 and humedad > 75:
            desc = random.choice(descripciones_lluvia)
        elif humedad > 50:
            desc = random.choice(descripciones_nubes)
        else:
            desc = random.choice(descripciones_despejado)
            
        visibilidad = 10000 if desc in descripciones_despejado else random.randint(7000, 9000)
        
        records.append((
            dt.isoformat(),
            "Juliaca",
            temperatura,
            round(sensacion, 2),
            round(temperatura - random.uniform(0.5, 1.2), 2),
            round(temperatura + random.uniform(0.5, 1.2), 2),
            humedad,
            presion,
            desc,
            viento,
            visibilidad,
            dt.hour,
            dt.weekday()
        ))
        
        if len(records) >= batch_size:
            con.executemany(sql, records)
            con.commit()
            count += len(records)
            print(f"Insertados {count}/{total_registros} registros...")
            records = []
            
        current_time += step
        
    if records:
        con.executemany(sql, records)
        con.commit()
        count += len(records)
        print(f"Insertados {count}/{total_registros} registros...")
        
    con.close()
    print("Datos históricos generados exitosamente.")

if __name__ == "__main__":
    respaldar_db()
    inicializar_db()
    generar_datos()
