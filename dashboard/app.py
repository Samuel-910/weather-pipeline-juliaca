"""
Dashboard Weather Juliaca — Tiempo Real
Usa Server-Sent Events (SSE): el navegador se actualiza
CADA VEZ que Kafka recibe un nuevo evento de Spark.
Abrir: http://localhost:5000
"""

import os
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import time
import queue
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from flask import Flask, render_template_string, jsonify, Response, stream_with_context, request
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "weather-events")

app = Flask(__name__)

estado = {
    "heatmap":       {h: {"temp": None, "count": 0} for h in range(24)},
    "serie_tiempo":  deque(maxlen=60),
    "humedad_serie": deque(maxlen=60),
    "viento_serie":  deque(maxlen=60),
    "presion_serie": deque(maxlen=60),
    "log":           deque(maxlen=12),
    "ultima":        None,
    "total":         0,
    "errores":       0,
    "conectado":     False,
    "latencias":     deque(maxlen=20),
    "batch_num":     0,
}
lock = threading.Lock()

# Estructura para el estado de los modelos predictivos de Machine Learning
estado_ml = {
    "entrenado": False,
    "error": "El modelo de ML aún no se ha inicializado.",
    "mejor_modelo_nombre": None,
    "mae_gb": None, "r2_gb": None,
    "mae_rf": None, "r2_rf": None,
    "mae_ridge": None, "r2_ridge": None,
    "mae_knn": None, "r2_knn": None,
    "total_eventos": 0,
    "importancias": {},
    "modelo_rf": None, "modelo_gb": None,
    "modelo_ridge": None, "modelo_knn": None,
    "mejor_modelo": None,
}

def entrenar_modelos_ml():
    global estado_ml
    estado_ml["error"] = "Entrenando 4 modelos con juliaca_horario_real.csv..."
    print("[ML] Iniciando entrenamiento de modelos...")
    try:
        import pandas as pd
        import os, pickle, json
        from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
        from sklearn.linear_model import Ridge
        from sklearn.neighbors import KNeighborsRegressor
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, r2_score
        import numpy as np

        csv_path = os.path.join(os.path.dirname(__file__), '..', 'ml', 'files', 'juliaca_horario_real.csv')
        if not os.path.exists(csv_path):
            estado_ml["entrenado"] = False
            estado_ml["error"] = "El CSV no existe."
            return

        df = pd.read_csv(csv_path)
        df = df.dropna(subset=['temperatura', 'hora'])
        total = len(df)
        estado_ml["total_eventos"] = total

        if total < 20:
            estado_ml["entrenado"] = False
            estado_ml["error"] = "Datos insuficientes"
            return

        df['dt'] = pd.to_datetime(df['hora'], utc=True).dt.tz_convert('America/Lima')
        df['hora_dia'] = df['dt'].dt.hour
        df['dia_semana'] = df['dt'].dt.weekday
        df['mes'] = df['dt'].dt.month
        df['semana_anio'] = df['dt'].dt.isocalendar().week.astype(int)

        features = ["hora_dia", "dia_semana", "mes", "semana_anio"]
        X = df[features].values
        y = df["temperatura"].values

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        ridge = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
        ridge.fit(X_train, y_train)
        pred_ridge = ridge.predict(X_test)

        rf = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)
        pred_rf = rf.predict(X_test)

        gb = HistGradientBoostingRegressor(random_state=42)
        gb.fit(X_train, y_train)
        pred_gb = gb.predict(X_test)

        knn = make_pipeline(StandardScaler(), KNeighborsRegressor(n_neighbors=5))
        knn.fit(X_train, y_train)
        pred_knn = knn.predict(X_test)

        estado_ml["mae_ridge"] = round(float(mean_absolute_error(y_test, pred_ridge)), 2)
        estado_ml["r2_ridge"] = round(float(r2_score(y_test, pred_ridge)), 3)
        estado_ml["mae_rf"] = round(float(mean_absolute_error(y_test, pred_rf)), 2)
        estado_ml["r2_rf"] = round(float(r2_score(y_test, pred_rf)), 3)
        estado_ml["mae_gb"] = round(float(mean_absolute_error(y_test, pred_gb)), 2)
        estado_ml["r2_gb"] = round(float(r2_score(y_test, pred_gb)), 3)
        estado_ml["mae_knn"] = round(float(mean_absolute_error(y_test, pred_knn)), 2)
        estado_ml["r2_knn"] = round(float(r2_score(y_test, pred_knn)), 3)

        metricas_mae = {
            "Ridge": estado_ml["mae_ridge"],
            "RandomForest": estado_ml["mae_rf"],
            "HistGradientBoosting": estado_ml["mae_gb"],
            "KNN": estado_ml["mae_knn"]
        }
        mejor_nombre = min(metricas_mae, key=metricas_mae.get)
        estado_ml["mejor_modelo_nombre"] = mejor_nombre
        
        if mejor_nombre == "Ridge": estado_ml["mejor_modelo"] = ridge
        elif mejor_nombre == "RandomForest": estado_ml["mejor_modelo"] = rf
        elif mejor_nombre == "HistGradientBoosting": estado_ml["mejor_modelo"] = gb
        elif mejor_nombre == "KNN": estado_ml["mejor_modelo"] = knn

        importancias = {}
        for f, imp in zip(features, rf.feature_importances_):
            importancias[f] = round(float(imp), 3)

        estado_ml["modelo_ridge"] = ridge
        estado_ml["modelo_rf"] = rf
        estado_ml["modelo_gb"] = gb
        estado_ml["modelo_knn"] = knn
        estado_ml["importancias"] = importancias
        estado_ml["entrenado"] = True
        estado_ml["error"] = None

        ml_dir = os.path.join(os.path.dirname(__file__), '..', 'ml')
        os.makedirs(ml_dir, exist_ok=True)
        with open(os.path.join(ml_dir, 'modelo_rf.pkl'), 'wb') as f: pickle.dump(rf, f)
        with open(os.path.join(ml_dir, 'modelo_gb.pkl'), 'wb') as f: pickle.dump(gb, f)
        with open(os.path.join(ml_dir, 'modelo_ridge.pkl'), 'wb') as f: pickle.dump(ridge, f)
        with open(os.path.join(ml_dir, 'modelo_knn.pkl'), 'wb') as f: pickle.dump(knn, f)
        
        meta = {
            "mejor_modelo_nombre": estado_ml["mejor_modelo_nombre"],
            "mae_gb": estado_ml["mae_gb"], "r2_gb": estado_ml["r2_gb"],
            "mae_rf": estado_ml["mae_rf"], "r2_rf": estado_ml["r2_rf"],
            "mae_ridge": estado_ml["mae_ridge"], "r2_ridge": estado_ml["r2_ridge"],
            "mae_knn": estado_ml["mae_knn"], "r2_knn": estado_ml["r2_knn"],
            "total_eventos": estado_ml["total_eventos"],
            "importancias": estado_ml["importancias"]
        }
        with open(os.path.join(ml_dir, 'model_metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        print(f"[ML] Modelos persistidos en disco. Mejor: {mejor_nombre}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        estado_ml["entrenado"] = False
        estado_ml["error"] = f"Error: {str(e)}"
    except Exception as e:
        estado_ml["entrenado"] = False
        estado_ml["error"] = f"Error de entrenamiento: {str(e)}"
        print(f"[ML ERROR] {estado_ml['error']}")

# Cola de eventos SSE — cada cliente tiene la suya
clientes_sse = []
clientes_lock = threading.Lock()


def push_evento(data: dict):
    """Envía un evento SSE a todos los clientes conectados."""
    payload = f"data: {json.dumps(data)}\n\n"
    with clientes_lock:
        muertos = []
        for q in clientes_sse:
            try:
                q.put_nowait(payload)
            except Exception:
                muertos.append(q)
        for q in muertos:
            clientes_sse.remove(q)


def consumir_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BROKER],
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            with lock:
                estado["conectado"] = True
            print(f"Kafka conectado -> {BROKER} | tópico: {TOPIC}")

            for msg in consumer:
                t0 = time.time()
                e  = msg.value
                hora  = int(e.get("hora_dia", datetime.now().hour))
                temp  = float(e.get("temperatura", 0))
                hum   = int(e.get("humedad", 0))
                viento= float(e.get("velocidad_viento", 0))
                pres  = int(e.get("presion", 0))
                ts_str = e.get("timestamp", datetime.now().astimezone().isoformat())
                ts_corto = ts_str[11:19]
                ts_local_full = ts_str
                
                desc  = e.get("descripcion", "")
                lat   = round((time.time() - t0) * 1000, 1)

                with lock:
                    estado["total"]    += 1
                    estado["batch_num"] = msg.offset
                    estado["ultima"]    = e
                    estado["ultima"]["timestamp_local"] = ts_local_full # Para el subtitulo
                    estado["latencias"].append(lat)

                    h = estado["heatmap"][hora]
                    prev = h["temp"] or temp
                    cnt  = h["count"]
                    h["temp"]  = round((prev * cnt + temp) / (cnt + 1), 2)
                    h["count"] = cnt + 1

                    estado["serie_tiempo"].append({"ts": ts_corto, "v": temp})
                    estado["humedad_serie"].append({"ts": ts_corto, "v": hum})
                    estado["viento_serie"].append({"ts": ts_corto, "v": viento})
                    estado["presion_serie"].append({"ts": ts_corto, "v": pres})
                    estado["log"].appendleft({
                        "ts": ts_corto, "temp": temp, "hum": hum,
                        "viento": viento, "desc": desc, "offset": msg.offset,
                    })

                    snap = {
                        "conectado": True,
                        "total":     estado["total"],
                        "batch":     msg.offset,
                        "lat_ms":    lat,
                        "lat_prom":  round(sum(estado["latencias"]) / len(estado["latencias"]), 1),
                        "ultima":    e,
                        "heatmap":   [{"h": h2, **estado["heatmap"][h2]} for h2 in range(24)],
                        "serie":     list(estado["serie_tiempo"]),
                        "hum_serie": list(estado["humedad_serie"]),
                        "viento_serie": list(estado["viento_serie"]),
                        "presion_serie": list(estado["presion_serie"]),
                        "log":       list(estado["log"])[:10],
                    }

                push_evento(snap)

        except Exception as ex:
            print(f"Error Kafka: {ex}")
            with lock:
                estado["conectado"] = False
                estado["errores"]  += 1
            time.sleep(5)


threading.Thread(target=consumir_kafka, daemon=True).start()


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/estado")
def api_estado():
    with lock:
        return jsonify({
            "conectado": estado["conectado"],
            "total":     estado["total"],
            "ultima":    estado["ultima"],
            "heatmap":   [{"h": h, **estado["heatmap"][h]} for h in range(24)],
            "serie":     list(estado["serie_tiempo"]),
            "hum_serie": list(estado["humedad_serie"]),
            "viento_serie": list(estado["viento_serie"]),
            "presion_serie": list(estado["presion_serie"]),
            "log":       list(estado["log"])[:10],
            "lat_prom":  round(sum(estado["latencias"]) / max(len(estado["latencias"]), 1), 1),
        })


@app.route("/stream")
def stream():
    """SSE endpoint — cada cliente se suscribe aquí."""
    q = queue.Queue(maxsize=50)
    with clientes_lock:
        clientes_sse.append(q)

    def generar():
        try:
            # Enviar estado inicial
            with lock:
                snap = {
                    "conectado": estado["conectado"],
                    "total":     estado["total"],
                    "ultima":    estado["ultima"],
                    "heatmap":   [{"h": h, **estado["heatmap"][h]} for h in range(24)],
                    "serie":     list(estado["serie_tiempo"]),
                    "hum_serie": list(estado["humedad_serie"]),
                    "viento_serie": list(estado["viento_serie"]),
                    "presion_serie": list(estado["presion_serie"]),
                    "log":       list(estado["log"])[:10],
                    "lat_prom":  0,
                }
            yield f"data: {json.dumps(snap)}\n\n"

            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with clientes_lock:
                if q in clientes_sse:
                    clientes_sse.remove(q)

    return Response(
        stream_with_context(generar()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/api/monitoreo-historial")
def api_monitoreo_historial():
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'ml', 'files', 'juliaca_horario_real.csv')
    if not os.path.exists(csv_path):
        return jsonify({"success": False, "error": "El archivo CSV no existe."}), 400

    try:
        filtro = request.args.get("filtro", "hoy")  # hoy, semana, mes, anio
        import pandas as pd
        
        df = pd.read_csv(csv_path)
        df['dt'] = pd.to_datetime(df['hora'])
        # Simular que el CSV es datos recientes, así que ajustamos fechas al "now" para que los filtros funcionen
        # Como es historico, el filtro "hoy" puede no coincidir con la fecha de la maquina.
        # Mejor agrupar todo relativo al max fecha del df.
        max_dt = df['dt'].max()
        
        if filtro == "hoy":
            df_filt = df[df['dt'].dt.date == max_dt.date()]
            grouped = df_filt.groupby(df_filt['dt'].dt.hour).agg({'temperatura': 'mean', 'presion': 'mean'}).reset_index()
            labels = [f"{int(h)}h" for h in grouped['dt']]
            temps = grouped['temperatura'].round(2).tolist()
            presiones = grouped['presion'].round(2).tolist()
            
            heatmap_dict = {h: {"temp": None, "count": 0} for h in range(24)}
            for _, r in df_filt.iterrows():
                hr = r['dt'].hour
                cnt = heatmap_dict[hr]["count"]
                prev = heatmap_dict[hr]["temp"] or 0
                heatmap_dict[hr]["temp"] = (prev * cnt + r['temperatura']) / (cnt + 1)
                heatmap_dict[hr]["count"] = cnt + 1
            
        elif filtro == "semana":
            min_dt = max_dt - pd.Timedelta(days=7)
            df_filt = df[df['dt'] >= min_dt]
            grouped = df_filt.groupby(df_filt['dt'].dt.date).agg({'temperatura': 'mean', 'presion': 'mean'}).reset_index()
            labels = [f"{d.day}/{d.month}" for d in grouped['dt']]
            temps = grouped['temperatura'].round(2).tolist()
            presiones = grouped['presion'].round(2).tolist()
            
            heatmap_dict = {h: {"temp": None, "count": 0} for h in range(24)}
            for hr, g in df_filt.groupby(df_filt['dt'].dt.hour):
                heatmap_dict[hr]["temp"] = g['temperatura'].mean()
                heatmap_dict[hr]["count"] = len(g)
                
        elif filtro == "mes":
            min_dt = max_dt - pd.Timedelta(days=30)
            df_filt = df[df['dt'] >= min_dt]
            grouped = df_filt.groupby(df_filt['dt'].dt.date).agg({'temperatura': 'mean', 'presion': 'mean'}).reset_index()
            labels = [f"{d.day}/{d.month}" for d in grouped['dt']]
            temps = grouped['temperatura'].round(2).tolist()
            presiones = grouped['presion'].round(2).tolist()
            
            heatmap_dict = {h: {"temp": None, "count": 0} for h in range(24)}
            for hr, g in df_filt.groupby(df_filt['dt'].dt.hour):
                heatmap_dict[hr]["temp"] = g['temperatura'].mean()
                heatmap_dict[hr]["count"] = len(g)
                
        elif filtro == "anio":
            # Agrupar por mes
            df['mes_anio'] = df['dt'].dt.strftime('%Y-%m')
            grouped = df.groupby('mes_anio').agg({'temperatura': 'mean', 'presion': 'mean'}).reset_index()
            meses_nombres = {"01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr", "05": "May", "06": "Jun", "07": "Jul", "08": "Ago", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic"}
            labels = [f"{meses_nombres.get(m[-2:], m[-2:])} {m[:4]}" for m in grouped['mes_anio']]
            temps = grouped['temperatura'].round(2).tolist()
            presiones = grouped['presion'].round(2).tolist()
            
            heatmap_dict = {h: {"temp": None, "count": 0} for h in range(24)}
            for hr, g in df.groupby(df['dt'].dt.hour):
                heatmap_dict[hr]["temp"] = g['temperatura'].mean()
                heatmap_dict[hr]["count"] = len(g)
        else:
            return jsonify({"success": False, "error": f"Filtro desconocido: {filtro}"}), 400

        heatmap_list = []
        for h in range(24):
            t = heatmap_dict[h]["temp"]
            heatmap_list.append({
                "h": h,
                "temp": round(t, 2) if pd.notnull(t) else None,
                "count": heatmap_dict[h]["count"]
            })

        return jsonify({
            "success": True,
            "labels": labels,
            "temperatura": temps,
            "presion": presiones,
            "heatmap": heatmap_list
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


# ─── Endpoints de Machine Learning ─────────────────────────────────────────────

@app.route("/api/ml-info")
def api_ml_info():
    return jsonify({
        "entrenado": estado_ml["entrenado"],
        "error": estado_ml["error"],
        "total_eventos": estado_ml["total_eventos"],
        "mejor_modelo_nombre": estado_ml["mejor_modelo_nombre"],
        "mae_gb": estado_ml["mae_gb"], "r2_gb": estado_ml["r2_gb"],
        "mae_rf": estado_ml["mae_rf"], "r2_rf": estado_ml["r2_rf"],
        "mae_ridge": estado_ml["mae_ridge"], "r2_ridge": estado_ml["r2_ridge"],
        "mae_knn": estado_ml["mae_knn"], "r2_knn": estado_ml["r2_knn"],
        "importancias": estado_ml["importancias"]
    })


@app.route("/api/ml-retrain", methods=["POST", "GET"])
def api_ml_retrain():
    entrenar_modelos_ml()
    return jsonify({
        "entrenado": estado_ml["entrenado"],
        "error": estado_ml["error"],
        "total_eventos": estado_ml["total_eventos"],
        "mejor_modelo_nombre": estado_ml["mejor_modelo_nombre"],
        "mae_rf": estado_ml["mae_rf"],
        "r2_rf": estado_ml["r2_rf"]
    })


@app.route("/api/predict")
def api_predict():
    if not estado_ml["entrenado"]:
        return jsonify({"success": False, "error": estado_ml["error"] or "El modelo predictivo no está listo."}), 400

    try:
        hora_dia = int(request.args.get("hora_dia", datetime.now().hour))
        dia_semana = int(request.args.get("dia_semana", datetime.now().weekday()))
        mes = int(request.args.get("mes", datetime.now().month))
        semana_anio = int(request.args.get("semana_anio", datetime.now().isocalendar()[1]))

        import numpy as np
        X_in = np.array([[hora_dia, dia_semana, mes, semana_anio]])

        mejor = estado_ml["mejor_modelo"]
        rf_model = estado_ml["modelo_rf"]
        gb_model = estado_ml["modelo_gb"]
        
        pred_mejor = round(float(mejor.predict(X_in)[0]), 2)
        pred_rf = round(float(rf_model.predict(X_in)[0]), 2)
        pred_gb = round(float(gb_model.predict(X_in)[0]), 2)

        return jsonify({
            "success": True,
            "inputs": {
                "hora_dia": hora_dia,
                "dia_semana": dia_semana,
                "mes": mes,
                "semana_anio": semana_anio
            },
            "predicciones": {
                "mejor_modelo": pred_mejor,
                "mejor_modelo_nombre": estado_ml["mejor_modelo_nombre"],
                "random_forest": pred_rf,
                "gradient_boosting": pred_gb
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"Error al predecir: {str(e)}"}), 500


@app.route("/api/predict-trend")
def api_predict_trend():
    if not estado_ml["entrenado"]:
        return jsonify({"success": False, "error": estado_ml["error"] or "El modelo predictivo no está listo."}), 400

    try:
        modo = request.args.get("modo", "dia")  # dia, semana, mes, anio
        hora_inicio = int(request.args.get("hora_inicio", 0))
        hora_fin = int(request.args.get("hora_fin", 23))
        
        fecha_str = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
        fecha_base = datetime.strptime(fecha_str, "%Y-%m-%d")
        
        dia_semana_base = (fecha_base.weekday()) % 7
        mes_base = fecha_base.month
        semana_anio_base = fecha_base.isocalendar()[1]
        
        import numpy as np
        mejor_model = estado_ml["mejor_modelo"]
        rf_model = estado_ml["modelo_rf"]
        gb_model = estado_ml["modelo_gb"]
        ridge_model = estado_ml.get("modelo_ridge")
        knn_model = estado_ml.get("modelo_knn")
        
        hora_inicio = max(0, min(23, hora_inicio))
        hora_fin = max(0, min(23, hora_fin))
        if hora_inicio > hora_fin:
            hora_inicio, hora_fin = hora_fin, hora_inicio
            
        horas_rango = np.arange(hora_inicio, hora_fin + 1)
        n_horas = len(horas_rango)
        
        if modo == "dia":
            X_in = np.column_stack([
                horas_rango,
                np.full(n_horas, dia_semana_base),
                np.full(n_horas, mes_base),
                np.full(n_horas, semana_anio_base)
            ])
            labels = [f"{h:02d}:00h" for h in horas_rango]
            
            preds_mejor = mejor_model.predict(X_in).tolist()
            preds_rf = rf_model.predict(X_in).tolist()
            preds_gb = gb_model.predict(X_in).tolist()
            
        elif modo == "semana":
            labels_semana = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            grid_list = []
            labels = []
            for i in range(7):
                date_d = fecha_base + timedelta(days=i)
                d = date_d.weekday()
                labels.append(labels_semana[d])
                for h in horas_rango:
                    grid_list.append([h, d, date_d.month, date_d.isocalendar()[1]])
            
            grid = np.array(grid_list)
            preds_mejor = mejor_model.predict(grid).reshape(7, n_horas).mean(axis=1).tolist()
            preds_rf = rf_model.predict(grid).reshape(7, n_horas).mean(axis=1).tolist()
            preds_gb = gb_model.predict(grid).reshape(7, n_horas).mean(axis=1).tolist()
            if ridge_model: preds_ridge = ridge_model.predict(grid).reshape(7, n_horas).mean(axis=1).tolist()
            if knn_model: preds_knn = knn_model.predict(grid).reshape(7, n_horas).mean(axis=1).tolist()
            
        elif modo == "mes":
            grid_list = []
            labels = []
            for i in range(30):
                date_d = fecha_base + timedelta(days=i)
                d = date_d.weekday()
                labels.append(f"{date_d.day}/{date_d.month}")
                for h in horas_rango:
                    grid_list.append([h, d, date_d.month, date_d.isocalendar()[1]])
                    
            grid = np.array(grid_list)
            preds_mejor = mejor_model.predict(grid).reshape(30, n_horas).mean(axis=1).tolist()
            preds_rf = rf_model.predict(grid).reshape(30, n_horas).mean(axis=1).tolist()
            preds_gb = gb_model.predict(grid).reshape(30, n_horas).mean(axis=1).tolist()
            if ridge_model: preds_ridge = ridge_model.predict(grid).reshape(30, n_horas).mean(axis=1).tolist()
            if knn_model: preds_knn = knn_model.predict(grid).reshape(30, n_horas).mean(axis=1).tolist()
            
        elif modo == "anio":
            semanas_mes = [2, 6, 11, 15, 20, 24, 28, 33, 37, 41, 46, 50]
            meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
            
            grid_list = []
            labels = []
            for i in range(12):
                m_calc = ((mes_base - 1 + i) % 12) + 1
                labels.append(meses_nombres[m_calc - 1])
                semana_m = semanas_mes[m_calc - 1]
                for d in range(7):
                    for h in horas_rango:
                        grid_list.append([h, d, m_calc, semana_m])
                        
            grid = np.array(grid_list)
            preds_mejor = mejor_model.predict(grid).reshape(12, 7 * n_horas).mean(axis=1).tolist()
            preds_rf = rf_model.predict(grid).reshape(12, 7 * n_horas).mean(axis=1).tolist()
            preds_gb = gb_model.predict(grid).reshape(12, 7 * n_horas).mean(axis=1).tolist()
            if ridge_model: preds_ridge = ridge_model.predict(grid).reshape(12, 7 * n_horas).mean(axis=1).tolist()
            if knn_model: preds_knn = knn_model.predict(grid).reshape(12, 7 * n_horas).mean(axis=1).tolist()
        else:
            return jsonify({"success": False, "error": f"Modo desconocido: {modo}"}), 400
            
        preds_mejor = [round(float(p), 2) for p in preds_mejor]
        preds_rf = [round(float(p), 2) for p in preds_rf]
        preds_gb = [round(float(p), 2) for p in preds_gb]
        
        return jsonify({
            "success": True,
            "inputs": {
                "modo": modo, "hora_inicio": hora_inicio, "hora_fin": hora_fin,
                "fecha": fecha_str, "dia_semana_base": dia_semana_base,
                "mes_base": mes_base, "semana_anio_base": semana_anio_base
            },
            "labels": labels,
            "mejor_modelo_nombre": estado_ml["mejor_modelo_nombre"],
            "predicciones": {
                "mejor_modelo": preds_mejor,
                "random_forest": preds_rf,
                "gradient_boosting": preds_gb
            },
            "stats": {
                "mejor_modelo": {"avg": round(float(np.mean(preds_mejor)), 2), "min": round(float(np.min(preds_mejor)), 2), "max": round(float(np.max(preds_mejor)), 2)},
                "random_forest": {"avg": round(float(np.mean(preds_rf)), 2), "min": round(float(np.min(preds_rf)), 2), "max": round(float(np.max(preds_rf)), 2)},
                "gradient_boosting": {"avg": round(float(np.mean(preds_gb)), 2), "min": round(float(np.min(preds_gb)), 2), "max": round(float(np.max(preds_gb)), 2)}
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"Error al predecir la tendencia: {str(e)}"}), 500


def cargar_modelos_ml():
    global estado_ml
    try:
        import pickle
        import json
        ml_dir = os.path.join(os.path.dirname(__file__), '..', 'ml')
        path_rf = os.path.join(ml_dir, 'modelo_rf.pkl')
        path_gb = os.path.join(ml_dir, 'modelo_gb.pkl')
        path_ridge = os.path.join(ml_dir, 'modelo_ridge.pkl')
        path_knn = os.path.join(ml_dir, 'modelo_knn.pkl')
        path_meta = os.path.join(ml_dir, 'model_metadata.json')

        if os.path.exists(path_rf) and os.path.exists(path_gb) and os.path.exists(path_meta):
            print("[ML] Cargando modelos persistidos desde disco...")
            with open(path_rf, 'rb') as f: rf = pickle.load(f)
            with open(path_gb, 'rb') as f: gb = pickle.load(f)
            if os.path.exists(path_ridge):
                with open(path_ridge, 'rb') as f: ridge = pickle.load(f)
            else: ridge = None
            if os.path.exists(path_knn):
                with open(path_knn, 'rb') as f: knn = pickle.load(f)
            else: knn = None
            
            with open(path_meta, 'r', encoding='utf-8') as f: meta = json.load(f)

            estado_ml["modelo_rf"] = rf
            estado_ml["modelo_gb"] = gb
            estado_ml["modelo_ridge"] = ridge
            estado_ml["modelo_knn"] = knn
            
            estado_ml["mejor_modelo_nombre"] = meta.get("mejor_modelo_nombre", "RandomForest")
            mejor_nombre = estado_ml["mejor_modelo_nombre"]
            if mejor_nombre == "Ridge" and ridge: estado_ml["mejor_modelo"] = ridge
            elif mejor_nombre == "RandomForest" and rf: estado_ml["mejor_modelo"] = rf
            elif mejor_nombre == "HistGradientBoosting" and gb: estado_ml["mejor_modelo"] = gb
            elif mejor_nombre == "KNN" and knn: estado_ml["mejor_modelo"] = knn
            else: estado_ml["mejor_modelo"] = rf

            estado_ml["mae_gb"] = meta.get("mae_gb")
            estado_ml["r2_gb"] = meta.get("r2_gb")
            estado_ml["mae_rf"] = meta.get("mae_rf")
            estado_ml["r2_rf"] = meta.get("r2_rf")
            estado_ml["mae_ridge"] = meta.get("mae_ridge")
            estado_ml["r2_ridge"] = meta.get("r2_ridge")
            estado_ml["mae_knn"] = meta.get("mae_knn")
            estado_ml["r2_knn"] = meta.get("r2_knn")
            estado_ml["importancias"] = meta.get("importancias", {})
            estado_ml["total_eventos"] = meta.get("total_eventos", 0)
            estado_ml["entrenado"] = True
            estado_ml["error"] = None
            print(f"[ML] Modelos cargados exitosamente desde disco. Mejor: {mejor_nombre}")
            return True
    except Exception as e:
        print(f"[ML ERROR] Error al cargar modelos persistidos: {str(e)}")
    return False


# Iniciar ML cargando modelos de disco, o entrenándolos en segundo plano si no existen
def init_ml():
    if not cargar_modelos_ml():
        print("[ML] No se encontraron modelos guardados en disco. Iniciando entrenamiento en segundo plano...")
        threading.Thread(target=entrenar_modelos_ml, daemon=True).start()

init_ml()


HTML = """<!DOCTYPE html>
<html lang="es" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pipeline Weather · Juliaca</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

:root[data-theme="dark"] {
  --bg: #0f172a;
  --bg2: #1e293b;
  --border: #334155;
  --text: #f8fafc;
  --text-muted: #94a3b8;
  --amber: #f59e0b;
  --amber-bg: rgba(245, 158, 11, 0.15);
  --blue: #3b82f6;
  --blue-bg: rgba(59, 130, 246, 0.15);
  --purple: #8b5cf6;
  --purple-bg: rgba(139, 92, 246, 0.15);
  --green: #10b981;
  --green-bg: rgba(16, 185, 129, 0.15);
  --red: #ef4444;
  --bar-bg: #334155;
  --card-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3), 0 4px 6px -4px rgba(0, 0, 0, 0.3);
  --glass: rgba(30, 41, 59, 0.7);
}
:root[data-theme="light"] {
  --bg: #f8fafc;
  --bg2: #ffffff;
  --border: #e2e8f0;
  --text: #0f172a;
  --text-muted: #64748b;
  --amber: #d97706;
  --amber-bg: rgba(217, 119, 6, 0.15);
  --blue: #2563eb;
  --blue-bg: rgba(37, 99, 235, 0.15);
  --purple: #7c3aed;
  --purple-bg: rgba(124, 58, 237, 0.15);
  --green: #059669;
  --green-bg: rgba(5, 150, 105, 0.15);
  --red: #dc2626;
  --bar-bg: #e2e8f0;
  --card-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.025);
  --glass: rgba(255, 255, 255, 0.7);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); padding: 20px; transition: background 0.3s, color 0.3s; }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding: 10px; }
h1 { font-size: 24px; font-weight: 700; letter-spacing: -0.5px; display: flex; align-items: center; gap: 10px; }
h1 i { color: var(--amber); }
.header p { color: var(--text-muted); font-size: 13px; margin-top: 4px; }
.header-right { display: flex; align-items: center; gap: 16px; }
.theme-switch { cursor: pointer; padding: 8px 12px; border-radius: 8px; background: var(--bg2); border: 1px solid var(--border); color: var(--text); font-size: 16px; transition: 0.2s; }
.theme-switch:hover { background: var(--border); }
.badge { display: flex; align-items: center; gap: 8px; padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; }
.badge-ok { background: rgba(16, 185, 129, 0.1); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.2); }
.badge-err { background: rgba(239, 68, 68, 0.1); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.2); }
.dot { width: 8px; height: 8px; border-radius: 50%; }
.dot-ok { background: var(--green); box-shadow: 0 0 8px var(--green); }
.dot-err { background: var(--red); box-shadow: 0 0 8px var(--red); }

/* Navigation Tabs */
.nav-tabs { display: flex; gap: 8px; margin-bottom: 24px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
.tab-btn { cursor: pointer; padding: 10px 18px; border-radius: 8px; font-size: 14px; font-weight: 600; color: var(--text-muted); transition: all 0.2s ease; display: flex; align-items: center; gap: 8px; border: 1px solid transparent; }
.tab-btn:hover { background: var(--bg2); color: var(--text); }
.tab-btn.active { background: var(--amber-bg); color: var(--amber); border-color: rgba(245, 158, 11, 0.3); }

.tab-content { display: none; animation: fadeIn 0.3s ease-in-out; }
.tab-content.active { display: block; }

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}

.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }
.mcard { background: var(--bg2); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: var(--card-shadow); transition: transform 0.2s; display: flex; flex-direction: column; justify-content: space-between; }
.mcard:hover { transform: translateY(-2px); border-color: var(--text-muted); }
.mcard-header { font-size: 13px; color: var(--text-muted); font-weight: 600; text-transform: uppercase; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
.mcard-header i { font-size: 16px; width: 20px; text-align: center; }

.val-text { font-size: 32px; font-weight: 700; letter-spacing: -1px; margin-bottom: 4px; }
.val-unit { font-size: 16px; color: var(--text-muted); font-weight: 500; }
.mcard-footer { font-size: 12px; color: var(--text-muted); border-top: 1px solid var(--border); padding-top: 12px; margin-top: auto; }

/* Grids / Rows */
.row { display: grid; gap: 16px; margin-bottom: 20px; }
.r2 { grid-template-columns: 1fr 1fr; }
@media(max-width:900px){ .r2 { grid-template-columns: 1fr; } }

.panel { background: var(--bg2); border: 1px solid var(--border); border-radius: 16px; padding: 20px; box-shadow: var(--card-shadow); position: relative; }
.panel h2 { font-size: 13px; font-weight: 600; color: var(--text-muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 18px; display: flex; align-items: center; gap: 8px; }

/* Donut Chart Inner Text */
.donut-container { position: relative; width: 100%; max-width: 300px; margin: 0 auto; }
.donut-inner { position: absolute; bottom: 10%; left: 0; right: 0; text-align: center; }
.donut-val { font-size: 36px; font-weight: 700; color: var(--text); line-height: 1; }
.donut-label { font-size: 12px; color: var(--text-muted); margin-top: 4px; }

/* Heatmap & Logs */
.hm-wrap { display: flex; flex-direction: column; gap: 2px; }
.hm-hours { display: flex; gap: 2px; padding-left: 0; margin-bottom: 2px; }
.hm-h { flex: 1; text-align: center; font-size: 10px; color: var(--text-muted); }
.hm-row { display: flex; gap: 2px; }
.hm-cell { flex: 1; height: 36px; border-radius: 4px; transition: transform .15s; }
.hm-cell:hover { transform: scaleY(1.15); opacity: .85; cursor: pointer; }

.log-wrap { font-family: 'Cascadia Code', monospace; font-size: 12px; line-height: 1.8; max-height: 200px; overflow-y: auto; }
.log-row { display: flex; gap: 12px; padding: 4px 0; border-bottom: 1px dashed var(--border); }
.log-ts { color: var(--text-muted); min-width: 70px; }
.log-desc { color: var(--text-muted); }

/* Form Controls for ML */
.form-group { margin-bottom: 18px; }
.form-label { display: flex; justify-content: space-between; font-size: 13px; font-weight: 600; color: var(--text-muted); margin-bottom: 8px; }
.form-label span.val { color: var(--amber); font-weight: 700; }
.form-input-range { width: 100%; height: 6px; background: var(--border); border-radius: 4px; outline: none; -webkit-appearance: none; transition: background 0.2s; }
.form-input-range::-webkit-slider-thumb { -webkit-appearance: none; width: 18px; height: 18px; border-radius: 50%; background: var(--amber); cursor: pointer; transition: transform 0.1s; }
.form-input-range::-webkit-slider-thumb:hover { transform: scale(1.2); }
.form-select { width: 100%; padding: 10px 12px; border-radius: 8px; background: var(--bg); border: 1px solid var(--border); color: var(--text); font-family: inherit; font-size: 14px; outline: none; transition: border-color 0.2s; }
.form-select:focus { border-color: var(--amber); }

.btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; width: 100%; padding: 12px 20px; font-size: 14px; font-weight: 600; text-align: center; border-radius: 8px; border: 1px solid transparent; cursor: pointer; transition: all 0.2s ease; }
.btn-primary { background: var(--amber); color: #000; }
.btn-primary:hover { background: #f5b02b; transform: translateY(-1px); }
.btn-secondary { background: var(--bg); border-color: var(--border); color: var(--text); }
.btn-secondary:hover { background: var(--border); }

/* ML Output Cards */
.ml-results { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
@media(max-width:450px){ .ml-results { grid-template-columns: 1fr; } }
.ml-res-card { padding: 20px; border-radius: 16px; border: 1px solid var(--border); display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; position: relative; overflow: hidden; background: var(--bg2); box-shadow: var(--card-shadow); }
.ml-res-card.rf { border-left: 4px solid var(--purple); background: linear-gradient(180deg, var(--purple-bg) 0%, var(--bg2) 100%); }
.ml-res-card.gb { border-left: 4px solid var(--green); background: linear-gradient(180deg, var(--green-bg) 0%, var(--bg2) 100%); }
.ml-res-val { font-size: 40px; font-weight: 700; letter-spacing: -1px; margin: 8px 0; }
.ml-res-title { font-size: 12px; text-transform: uppercase; font-weight: 700; color: var(--text-muted); display: flex; align-items: center; gap: 6px; }

/* Features Importance simple bars */
.feat-list { display: flex; flex-direction: column; gap: 12px; margin-top: 10px; }
.feat-item { display: flex; align-items: center; font-size: 12px; }
.feat-name { width: 130px; color: var(--text-muted); font-weight: 500; text-transform: capitalize; }
.feat-bar-wrap { flex: 1; height: 8px; background: var(--bar-bg); border-radius: 4px; overflow: hidden; margin: 0 12px; }
.feat-bar { height: 100%; background: var(--amber); border-radius: 4px; width: 0%; transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1); }
.feat-val { width: 40px; text-align: right; font-weight: 600; color: var(--text); }

/* Alerts and Loading */
.alert { padding: 16px; border-radius: 12px; border: 1px solid transparent; font-size: 13px; line-height: 1.5; display: flex; align-items: flex-start; gap: 12px; margin-bottom: 20px; }
.alert-warning { background: var(--amber-bg); color: var(--amber); border-color: rgba(245, 158, 11, 0.25); }
.alert-warning i { font-size: 18px; margin-top: 1px; }

.loading-overlay { display: none; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: rgba(15, 23, 42, 0.85); z-index: 10; border-radius: 16px; align-items: center; justify-content: center; flex-direction: column; gap: 12px; }
.spinner { width: 40px; height: 40px; border: 4px solid var(--border); border-top-color: var(--amber); border-radius: 50%; animation: spin 1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

/* Table for Metrics */
.metrics-table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 12px; }
.metrics-table th, .metrics-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
.metrics-table th { color: var(--text-muted); font-weight: 600; text-transform: uppercase; font-size: 11px; }
.metrics-table td { font-weight: 500; }

::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <h1><i class="fa-solid fa-cloud-sun-rain"></i> Weather Pipeline — Juliaca</h1>
    <p id="sub-ts">Esperando datos de Kafka + Spark...</p>
  </div>
  <div class="header-right">
    <div class="theme-switch" id="theme-btn" title="Modo Claro/Oscuro"><i class="fa-solid fa-moon"></i></div>
    <span id="badge-status" class="badge badge-err">
      <span class="dot dot-err" id="dot"></span>
      <span id="badge-txt">Sin conexión</span>
    </span>
  </div>
</div>

<!-- Tabs Navigation -->
<div class="nav-tabs">
  <div class="tab-btn active" id="tab-monitoreo" onclick="switchTab('monitoreo')"><i class="fa-solid fa-chart-line"></i> Monitoreo en Vivo</div>
  <div class="tab-btn" id="tab-prediccion" onclick="switchTab('prediccion')"><i class="fa-solid fa-brain"></i> Predicciones ML</div>
</div>

<!-- ───────────────── TAB: MONITOREO ───────────────── -->
<div id="section-monitoreo" class="tab-content active">
  <!-- Selector Temporal General de Monitoreo -->
  <div class="row" style="margin-bottom: 20px;">
    <div class="panel" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 20px; border-radius: 12px; flex-wrap: wrap; gap: 10px; width: 100%;">
      <div style="font-weight: 600; font-size: 13px; color: var(--text-muted); display: flex; align-items: center; gap: 8px; text-transform: uppercase; letter-spacing: .05em;">
        <i class="fa-solid fa-clock-rotate-left" style="color: var(--amber);"></i> Filtro Temporal del Monitoreo
      </div>
      <select class="form-select" id="monitoreo-filtro" style="width: auto; padding: 6px 12px; margin: 0; font-size: 13px;" onchange="cambiarFiltroMonitoreo(this.value)">
        <option value="vivo" selected>Tiempo Real (En Vivo)</option>
        <option value="hoy">Hoy (24 Horas)</option>
        <option value="semana">Esta Semana (7 Días)</option>
        <option value="mes">Este Mes (30 Días)</option>
        <option value="anio">Este Año (12 Meses)</option>
      </select>
    </div>
  </div>

  <!-- Row: Gauges -->
  <div class="row r2">
    <div class="panel">
      <h2><i class="fa-solid fa-temperature-half" style="color:var(--amber)"></i> Temperatura Actual</h2>
      <div class="donut-container">
        <canvas id="gauge-temp"></canvas>
        <div class="donut-inner">
          <div class="donut-val" id="m-temp">—°</div>
          <div class="donut-label">Sensación: <span id="m-sens">—°C</span></div>
        </div>
      </div>
    </div>
    <div class="panel">
      <h2><i class="fa-solid fa-droplet" style="color:var(--blue)"></i> Humedad Relativa</h2>
      <div class="donut-container">
        <canvas id="gauge-hum"></canvas>
        <div class="donut-inner">
          <div class="donut-val" id="m-hum">—%</div>
          <div class="donut-label" id="m-hum-desc">Cargando...</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Metrics Cards -->
  <div class="metrics">
    <div class="mcard">
      <div class="mcard-header"><i class="fa-solid fa-wind" style="color:var(--purple)"></i> Viento</div>
      <div class="val-text"><span id="m-viento">—</span> <span class="val-unit">m/s</span></div>
      <div class="mcard-footer">Velocidad media.<br><span id="m-viento-t">Normal</span></div>
    </div>
    <div class="mcard">
      <div class="mcard-header"><i class="fa-solid fa-gauge-high" style="color:var(--green)"></i> Presión</div>
      <div class="val-text"><span id="m-pres">—</span> <span class="val-unit">hPa</span></div>
      <div class="mcard-footer">Altitud de Juliaca: ~3820m.<br>Normal: ~630hPa</div>
    </div>
    <div class="mcard">
      <div class="mcard-header"><i class="fa-solid fa-database" style="color:var(--text-muted)"></i> Eventos Kafka</div>
      <div class="val-text"><span id="m-total">0</span></div>
      <div class="mcard-footer">Desde el inicio del stream.<br>Offset: <span id="m-offset">—</span></div>
    </div>
    <div class="mcard">
      <div class="mcard-header"><i class="fa-solid fa-bolt" style="color:var(--amber)"></i> Latencia</div>
      <div class="val-text"><span id="m-lat">—</span> <span class="val-unit">ms</span></div>
      <div class="mcard-footer">Tiempo de procesamiento.<br><span id="m-lat-t">Óptima</span></div>
    </div>
  </div>

  <!-- Line Charts -->
  <div class="row r2">
    <div class="panel">
      <h2><i class="fa-solid fa-chart-line" style="color:var(--amber)"></i> Histórico de Temperatura</h2>
      <canvas id="chart-temp" height="100"></canvas>
    </div>
    <div class="panel">
      <h2><i class="fa-solid fa-chart-line" style="color:var(--green)"></i> Presión Atmosférica</h2>
      <canvas id="chart-pres" height="100"></canvas>
    </div>
  </div>

  <div class="row r2">
    <div class="panel">
      <h2><i class="fa-solid fa-calendar-day" style="color:var(--amber)"></i> Mapa de Calor (24h)</h2>
      <div class="hm-wrap">
        <div class="hm-hours" id="hm-hdr"></div>
        <div class="hm-row" id="hm-row"></div>
      </div>
    </div>
    <div class="panel">
      <h2><i class="fa-solid fa-terminal" style="color:var(--text-muted)"></i> Log de Eventos en Vivo</h2>
      <div class="log-wrap" id="log-box"></div>
    </div>
  </div>
</div>

<!-- ───────────────── TAB: PREDICCIONES ML ───────────────── -->
<div id="section-prediccion" class="tab-content">
  <div class="row r2">
    <!-- Panel Izquierdo: Formularios -->
    <div class="panel">
      <h2><i class="fa-solid fa-calendar-days" style="color:var(--amber)"></i> Parámetros Temporales de Predicción</h2>
      
      <div class="form-group">
        <div class="form-label">Rango de Visualización</div>
        <select class="form-select" id="ml-chart-modo" onchange="cambiarModoGrafico(this.value)">
          <option value="dia" selected>Día (Por Horas)</option>
          <option value="semana">Semana (Por Días)</option>
          <option value="mes">Mes (Por Días del Mes)</option>
        </select>
      </div>

      <div class="form-group" id="group-fecha">
        <div class="form-label">Fecha a Estimar</div>
        <input type="date" class="form-select" id="param-fecha">
      </div>

      <div class="form-group" id="group-horas" style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
        <div>
          <div class="form-label">Desde las</div>
          <select class="form-select" id="param-hora-inicio" onchange="updHras()"></select>
        </div>
        <div>
          <div class="form-label">Hasta las</div>
          <select class="form-select" id="param-hora-fin" onchange="updHras()"></select>
        </div>
      </div>

      <div style="margin-top: 36px;">
        <button class="btn btn-primary" onclick="calcularPrediccion()" id="btn-calc"><i class="fa-solid fa-wand-magic-sparkles"></i> Predecir Temperatura</button>
      </div>
    </div>

    <!-- Panel Derecho: Resultados -->
    <div class="panel" id="panel-ml-resultados">
      <div class="loading-overlay" id="ml-loading">
        <div class="spinner"></div>
        <div style="font-weight: 600; font-size:14px; color:var(--text)" id="ml-loading-txt">Calculando estimación...</div>
      </div>
      
      <h2><i class="fa-solid fa-square-poll-vertical" style="color:var(--green)"></i> Resultados de Modelos</h2>
      
      <!-- Advertencia si no está entrenado -->
      <div id="ml-alert-insuficiente" class="alert alert-warning" style="display:none;">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <div>
          <strong>¡Modelo no listo!</strong><br>
          <span id="ml-alert-msg">Se necesitan al menos 20 registros históricos en SQLite para poder entrenar el modelo.</span>
          <br><br>
          <em>Sugerencia: Abre Docker Desktop, levanta Kafka e inicia el productor de clima para acumular eventos.</em>
        </div>
      </div>

      <div id="ml-visualizacion-grupo">
        <div class="ml-results">
          <div class="ml-res-card rf" style="background: var(--amber-bg); border-color: var(--amber);">
            <div class="ml-res-title" style="color: var(--amber);"><i class="fa-solid fa-trophy"></i> Mejor Modelo: <span id="nombre-mejor-modelo"></span></div>
            <div class="ml-res-val" id="pred-val-mejor" style="color: var(--amber);">—°C</div>
            <div id="pred-sub-mejor" style="font-size:11px; color:var(--text-muted); margin-bottom: 4px;">Seleccionado automáticamente por menor error (MAE)</div>
          </div>
        </div>

        <h2 style="margin-top:24px;"><i class="fa-solid fa-circle-info" style="color:var(--blue)"></i> Precisión y Datos de Validación</h2>
        <table class="metrics-table">
          <thead>
            <tr>
              <th>Algoritmo</th>
              <th>Error Medio (MAE)</th>
              <th>Ajuste (R²)</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td style="color:var(--purple); font-weight:700;">Random Forest</td>
              <td id="metric-mae-rf">—</td>
              <td id="metric-r2-rf">—</td>
            </tr>
            <tr>
              <td style="color:var(--green); font-weight:700;">Gradient Boosting</td>
              <td id="metric-mae-gb">—</td>
              <td id="metric-r2-gb">—</td>
            </tr>
            <tr>
              <td style="color:var(--blue); font-weight:700;">Ridge</td>
              <td id="metric-mae-ridge">—</td>
              <td id="metric-r2-ridge">—</td>
            </tr>
            <tr>
              <td style="color:var(--red); font-weight:700;">KNN</td>
              <td id="metric-mae-knn">—</td>
              <td id="metric-r2-knn">—</td>
            </tr>
          </tbody>
        </table>
        <div style="font-size:11px; color:var(--text-muted); margin-top:8px;" id="ml-total-txt">
          Datos de entrenamiento: — eventos históricos.
        </div>
      </div>

      <div style="margin-top:20px; border-top: 1px solid var(--border); padding-top:16px;">
        <button class="btn btn-secondary" onclick="reentrenarML()" id="btn-retrain" style="font-size:12px; padding: 8px 12px;"><i class="fa-solid fa-rotate"></i> Re-entrenar con Datos Frescos</button>
      </div>
    </div>
  </div>

  <!-- Nuevo Panel: Gráfico 24h de Variación -->
  <div class="row r2" style="margin-top: 24px;" id="ml-chart-container">
    <div class="panel" style="flex: 1; width: 100%;">
      <h2><i class="fa-solid fa-chart-line" style="color:var(--amber)"></i> Curva de Temperatura Estimada</h2>
      <div style="position: relative; height: 220px; width: 100%;">
        <canvas id="chart-ml-24h"></canvas>
      </div>
    </div>
  </div>
</div>

<script>
// Pestañas
function switchTab(tab) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  
  if (tab === 'monitoreo') {
    document.getElementById('tab-monitoreo').classList.add('active');
    document.getElementById('section-monitoreo').classList.add('active');
  } else {
    document.getElementById('tab-prediccion').classList.add('active');
    document.getElementById('section-prediccion').classList.add('active');
    cargarMLInfo(); // Actualiza el estado al entrar
  }
}

let cML24hLine = null;
let cachePredicciones = null;
let graficoModo = 'dia'; // 'dia', 'semana', 'mes', 'anio'

function initMLChart() {
  const ctx = document.getElementById('chart-ml-24h');
  if (!ctx) return;
  
  cML24hLine = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        {
          label: 'Mejor Modelo',
          data: [],
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245, 158, 11, 0.1)',
          fill: true,
          tension: 0.4,
          borderWidth: 3,
          pointRadius: 4,
          pointHoverRadius: 6,
          pointBackgroundColor: '#f59e0b'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'top', labels: { color: 'var(--text)', font: { size: 12, weight: '500' } } },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: 'rgba(15, 23, 42, 0.9)',
          titleColor: '#f8fafc',
          bodyColor: '#e2e8f0',
          borderColor: '#334155',
          borderWidth: 1,
          padding: 10,
          callbacks: { label: function(c) { return ' ' + c.dataset.label + ': ' + c.parsed.y + ' °C'; } }
        }
      },
      scales: {
        x: { grid: { color: 'var(--border)' }, ticks: { color: 'var(--text-muted)' } },
        y: {
          title: { display: true, text: 'Temperatura (°C)', color: 'var(--text-muted)' },
          grid: { color: 'var(--border)' },
          ticks: { color: 'var(--text)' }
        }
      },
      interaction: {
        mode: 'index',
        intersect: false
      }
    }
  });
}

// Configurar fecha de hoy en el input por defecto y poblar selectores
document.addEventListener("DOMContentLoaded", () => {
  const today = new Date().toISOString().split('T')[0];
  const dateInput = document.getElementById('param-fecha');
  if (dateInput) {
    dateInput.value = today;
    dateInput.addEventListener('change', () => {
      cachePredicciones = null; // Invalida caché si cambia la fecha
      debounceCalcular();
    });
  }
  
  const modoSelect = document.getElementById('ml-chart-modo');
  if (modoSelect) cambiarModoGrafico(modoSelect.value);
  
  // Poblar selectores de hora inicio y fin
  const startSel = document.getElementById('param-hora-inicio');
  const endSel = document.getElementById('param-hora-fin');
  if (startSel && endSel) {
    for (let h = 0; h < 24; h++) {
      const opt1 = document.createElement('option');
      opt1.value = h;
      opt1.textContent = `${h.toString().padStart(2, '0')}:00h`;
      if (h === 0) opt1.selected = true;
      startSel.appendChild(opt1);
      
      const opt2 = document.createElement('option');
      opt2.value = h;
      opt2.textContent = `${h.toString().padStart(2, '0')}:00h`;
      if (h === 23) opt2.selected = true;
      endSel.appendChild(opt2);
    }
  }
  
  initMLChart();
});

function updHras() {
  const startSel = document.getElementById('param-hora-inicio');
  const endSel = document.getElementById('param-hora-fin');
  if (startSel && endSel) {
    let start = parseInt(startSel.value);
    let end = parseInt(endSel.value);
    if (start > end) {
      endSel.value = start;
    }
  }
  cachePredicciones = null;
  debounceCalcular();
}

function cambiarModoGrafico(modo) {
  graficoModo = modo;
  const grpHoras = document.getElementById('group-horas');
  if (modo === 'dia') {
    if (grpHoras) grpHoras.style.display = 'grid';
  } else {
    if (grpHoras) grpHoras.style.display = 'none';
  }
  cachePredicciones = null; // Invalida caché al cambiar modo
  debounceCalcular();
}

function updLbl(id, val) {
  // Mantener por compatibilidad si es necesario
}

let debounceTimer;
function debounceCalcular() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    calcularPrediccion(true); // Modo silencioso
  }, 150);
}

// Obtener semana ISO en JS
function getWeekNumber(d) {
  d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay()||7));
  var yearStart = new Date(Date.UTC(d.getUTCFullYear(),0,1));
  var weekNo = Math.ceil(( ( (d - yearStart) / 86400000) + 1)/7);
  return weekNo;
}

// Cargar estado inicial de ML
function cargarMLInfo() {
  fetch('/api/ml-info')
    .then(r => r.json())
    .then(d => {
      const alertDiv = document.getElementById('ml-alert-insuficiente');
      const visualDiv = document.getElementById('ml-visualizacion-grupo');
      const chartContainer = document.getElementById('ml-chart-container');
      
      if (!d.entrenado) {
        alertDiv.style.display = 'block';
        document.getElementById('ml-alert-msg').textContent = d.error || 'Datos insuficientes.';
        visualDiv.style.display = 'none';
        if (chartContainer) chartContainer.style.display = 'none';
      } else {
        alertDiv.style.display = 'none';
        visualDiv.style.display = 'block';
        if (chartContainer) chartContainer.style.display = 'block';
        
        document.getElementById('metric-mae-rf').textContent = d.mae_rf + ' °C';
        document.getElementById('metric-r2-rf').textContent = d.r2_rf;
        document.getElementById('metric-mae-gb').textContent = d.mae_gb + ' °C';
        document.getElementById('metric-r2-gb').textContent = d.r2_gb;
        
        let mRidge = document.getElementById('metric-mae-ridge');
        if(mRidge) mRidge.textContent = d.mae_ridge + ' °C';
        let r2Ridge = document.getElementById('metric-r2-ridge');
        if(r2Ridge) r2Ridge.textContent = d.r2_ridge;
        
        let mKnn = document.getElementById('metric-mae-knn');
        if(mKnn) mKnn.textContent = d.mae_knn + ' °C';
        let r2Knn = document.getElementById('metric-r2-knn');
        if(r2Knn) r2Knn.textContent = d.r2_knn;

        document.getElementById('nombre-mejor-modelo').textContent = d.mejor_modelo_nombre;
        document.getElementById('ml-total-txt').textContent = `Datos de entrenamiento: ${d.total_eventos} eventos históricos de CSV (80% entrenamiento, 20% test).`;
        
        const box = document.getElementById('feat-list-box');
        box.innerHTML = '';
        const sorted = Object.entries(d.importancias).sort((a,b) => b[1] - a[1]);
        sorted.forEach(([k, v]) => {
          const friendlyNames = {
            "hora_dia": "Hora del Día",
            "dia_semana": "Día de la Semana",
            "mes": "Mes del Año",
            "semana_anio": "Semana del Año"
          };
          const pct = (v * 100).toFixed(0);
          box.innerHTML += `
            <div class="feat-item">
              <span class="feat-name">${friendlyNames[k] || k}</span>
              <div class="feat-bar-wrap">
                <div class="feat-bar" style="width: ${pct}%"></div>
              </div>
              <span class="feat-val">${pct}%</span>
            </div>
          `;
        });
        calcularPrediccion(true);
      }
    })
    .catch(() => {});
}


// Calcular Predicción de Temperatura
function calcularPrediccion(silencioso = false) {
  const loading = document.getElementById('ml-loading');
  const alertDiv = document.getElementById('ml-alert-insuficiente');
  if (alertDiv && alertDiv.style.display === 'block') return;

  const startSel = document.getElementById('param-hora-inicio');
  const endSel = document.getElementById('param-hora-fin');
  let rawDate = document.getElementById('param-fecha').value;
  
  let horaInicio = 0;
  let horaFin = 23;

  if (!rawDate) {
    if (!silencioso && loading) loading.style.display = 'none';
    return;
  }

  if (graficoModo === 'dia') {
    if (!startSel || !endSel) {
      if (!silencioso && loading) loading.style.display = 'none';
      return;
    }
    horaInicio = parseInt(startSel.value);
    horaFin = parseInt(endSel.value);
  }

  const cacheKey = `${graficoModo}-${horaInicio}-${horaFin}-${rawDate}`;

  if (cachePredicciones && cachePredicciones.key === cacheKey) {
    aplicarPredicciones(cachePredicciones.data);
    return;
  }

  if (!silencioso && loading) {
    const txt = document.getElementById('ml-loading-txt');
    if(txt) txt.textContent = 'Calculando tendencia...';
    loading.style.display = 'flex';
  }

  fetch(`/api/predict-trend?modo=${graficoModo}&hora_inicio=${horaInicio}&hora_fin=${horaFin}&fecha=${rawDate}`)
    .then(r => r.json())
    .then(d => {
      if (d.success) {
        cachePredicciones = {
          key: cacheKey,
          data: d
        };
        aplicarPredicciones(d);
      }
    })
    .finally(() => {
      if (!silencioso && loading) loading.style.display = 'none';
    });
}

function aplicarPredicciones(d) {
  const predsMejor = d.predicciones.mejor_modelo;
  const labels = d.labels;

  if (cML24hLine) {
    cML24hLine.data.labels = labels;
    cML24hLine.data.datasets[0].label = `Mejor Modelo (${d.mejor_modelo_nombre})`;
    cML24hLine.data.datasets[0].data = predsMejor;
    cML24hLine.update();
  }

  const start = d.inputs.hora_inicio;
  const end = d.inputs.hora_fin;

  const setIfExist = (id, text) => {
    let el = document.getElementById(id);
    if(el) el.textContent = text;
  };

  if (d.inputs.modo === 'dia') {
    if (start === end) {
      setIfExist('pred-val-mejor', predsMejor[0].toFixed(1) + ' °C');
      setIfExist('pred-sub-mejor', 'Estimación horaria');
    } else {
      setIfExist('pred-val-mejor', d.stats.mejor_modelo.avg.toFixed(1) + ' °C');
      setIfExist('pred-sub-mejor', `Promedio (Mín: ${d.stats.mejor_modelo.min}° / Máx: ${d.stats.mejor_modelo.max}°)`);
    }
  } else {
    setIfExist('pred-val-mejor', d.stats.mejor_modelo.avg.toFixed(1) + ' °C');
    const labelModo = d.inputs.modo === 'semana' ? 'semanal' : (d.inputs.modo === 'mes' ? 'mensual' : 'anual');
    setIfExist('pred-sub-mejor', `Tendencia ${labelModo} (Mín: ${d.stats.mejor_modelo.min}° / Máx: ${d.stats.mejor_modelo.max}°)`);
  }
}

// Re-entrenar modelo
function reentrenarML() {
  const btn = document.getElementById('btn-retrain');
  const loading = document.getElementById('ml-loading');
  
  btn.disabled = true;
  btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Entrenando...';
  document.getElementById('ml-loading-txt').textContent = 'Entrenando modelos de regresión...';
  loading.style.display = 'flex';

  fetch('/api/ml-retrain')
    .then(r => r.json())
    .then(d => {
      if (d.entrenado) {
        cachePredicciones = null; // Invalida caché de predicción tras re-entrenamiento
        alert('Modelos re-entrenados con éxito con los últimos datos de SQLite.');
      } else {
        alert('Error: ' + d.error);
      }
      cargarMLInfo();
    })
    .catch(() => { alert('No se pudo re-entrenar el modelo.'); })
    .finally(() => {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-rotate"></i> Re-entrenar con Datos Frescos';
      loading.style.display = 'none';
    });
}

// Theme Toggle
const themeBtn = document.getElementById('theme-btn');
const htmlEl = document.documentElement;
const savedTheme = localStorage.getItem('theme') || 'dark';
htmlEl.setAttribute('data-theme', savedTheme);
themeBtn.innerHTML = savedTheme === 'dark' ? '<i class="fa-solid fa-sun"></i>' : '<i class="fa-solid fa-moon"></i>';

themeBtn.addEventListener('click', () => {
  const current = htmlEl.getAttribute('data-theme');
  const next = current === 'dark' ? 'light' : 'dark';
  htmlEl.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  themeBtn.innerHTML = next === 'dark' ? '<i class="fa-solid fa-sun"></i>' : '<i class="fa-solid fa-moon"></i>';
  updateChartColors();
});

// Chart.js Default colors based on theme
function updateChartColors() {
    const isLight = htmlEl.getAttribute('data-theme') === 'light';
    Chart.defaults.color = isLight ? '#64748b' : '#94a3b8';
    Chart.defaults.borderColor = isLight ? '#e2e8f0' : '#232e48';
    if(cTempLine) cTempLine.update();
    if(cPresLine) cPresLine.update();
}

// Gauges (Doughnut) initialization
function mkGauge(id, color, maxVal) {
    const ctx = document.getElementById(id).getContext('2d');
    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [0, maxVal],
                backgroundColor: [color, 'rgba(0,0,0,0.1)'],
                borderWidth: 0,
                cutout: '80%',
                circumference: 180,
                rotation: 270
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: { tooltip: { enabled: false }, legend: { display: false } },
            animation: { duration: 500, animateScale: true }
        }
    });
}

// Line Charts initialization
function mkLine(id, color, min, max) {
  return new Chart(document.getElementById(id).getContext('2d'), {
    type: 'line',
    data: { labels: [], datasets: [{ data: [], borderColor: color, backgroundColor: color + '22', borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, tension: 0.4, fill: true }]},
    options: { animation: false, responsive: true, plugins: { legend: { display: false } },
      scales: { x: { ticks: { maxTicksLimit: 6 } }, y: { min, max } },
      interaction: { mode: 'index', intersect: false }
    }
  });
}

let monitoreoModo = 'vivo';
let cGaugeTemp, cGaugeHum, cTempLine, cPresLine;
setTimeout(() => {
    cGaugeTemp = mkGauge('gauge-temp', '#f59e0b', 30); // Max temp gauge ~30
    cGaugeHum = mkGauge('gauge-hum', '#3b82f6', 100);  // Max hum 100%
    cTempLine = mkLine('chart-temp', '#f59e0b', -5, 25);
    cPresLine = mkLine('chart-pres', '#10b981', 600, 680);
    updateChartColors();
}, 100);

// Heatmap Setup
const hdr = document.getElementById('hm-hdr');
const hmRow = document.getElementById('hm-row');
const hmCells = [];
for (let h = 0; h < 24; h++) {
  hdr.innerHTML += `<div class="hm-h">${h%3===0 ? h+'h' : ''}</div>`;
  const c = document.createElement('div');
  c.className = 'hm-cell';
  c.style.background = 'var(--bar-bg)';
  hmRow.appendChild(c);
  hmCells.push(c);
}

function tempColor(v) {
  if (v === null || v === undefined) return 'var(--bar-bg)';
  const t = Math.max(0, Math.min(1, (v - 2) / 18));
  if (t < 0.33) {
    const s = t / 0.33; return `rgb(${Math.round(30+s*0)},${Math.round(58+s*100)},${Math.round(95+s*22)})`;
  } else if (t < 0.66) {
    const s = (t-.33)/.33; return `rgb(${Math.round(30+s*215)},${Math.round(158-s*17)},${Math.round(117-s*58)})`;
  } else {
    const s = (t-.66)/.34; return `rgb(${Math.round(245-s*6)},${Math.round(158-s*97)},${Math.round(59-s*11)})`;
  }
}

function updLine(chart, serie) {
  if(!chart) return;
  const pts = serie.slice(-40);
  chart.data.labels = pts.map(p => p.ts);
  chart.data.datasets[0].data = pts.map(p => p.v);
  chart.update('none');
}

function cambiarFiltroMonitoreo(filtro) {
  monitoreoModo = filtro;
  if (filtro === 'vivo') {
    fetch('/api/estado')
      .then(r => r.json())
      .then(d => {
        aplicar(d);
      })
      .catch(() => {});
  } else {
    fetch(`/api/monitoreo-historial?filtro=${filtro}`)
      .then(r => r.json())
      .then(d => {
        if (d.success) {
          if (cTempLine) {
            cTempLine.data.labels = d.labels;
            cTempLine.data.datasets[0].data = d.temperatura;
            cTempLine.update('none');
          }
          if (cPresLine) {
            cPresLine.data.labels = d.labels;
            cPresLine.data.datasets[0].data = d.presion;
            cPresLine.update('none');
          }
          if (d.heatmap) {
            d.heatmap.forEach(h => {
              if (hmCells[h.h]) hmCells[h.h].style.background = tempColor(h.temp);
            });
          }
        }
      })
      .catch(() => {});
  }
}

// App Logic
function aplicar(d) {
  const ok = d.conectado;
  document.getElementById('badge-status').className = 'badge ' + (ok ? 'badge-ok' : 'badge-err');
  document.getElementById('badge-txt').textContent = ok ? 'Kafka Activo' : 'Sin conexión';
  document.getElementById('dot').className = 'dot ' + (ok ? 'dot-ok' : 'dot-err');

  if (d.ultima) {
    const u = d.ultima;
    document.getElementById('sub-ts').textContent = `Última actualización: ${(u.timestamp_local||u.timestamp||'').slice(11,19)} Local · ${u.ciudad||'Juliaca'}`;
    
    // Gauges Update
    document.getElementById('m-temp').textContent = u.temperatura + '°';
    document.getElementById('m-sens').textContent = u.sensacion_termica;
    if(cGaugeTemp) {
        const val = Math.max(0, u.temperatura);
        cGaugeTemp.data.datasets[0].data = [val, Math.max(0, 30 - val)];
        cGaugeTemp.update('none');
    }

    document.getElementById('m-hum').textContent = u.humedad + '%';
    document.getElementById('m-hum-desc').textContent = u.humedad > 90 ? 'Humedad Alta' : 'Humedad Normal';
    if(cGaugeHum) {
        cGaugeHum.data.datasets[0].data = [u.humedad, 100 - u.humedad];
        cGaugeHum.update('none');
    }

    // Cards Update
    document.getElementById('m-viento').textContent = u.velocidad_viento;
    document.getElementById('m-viento-t').textContent = u.velocidad_viento > 10 ? 'Fuerte' : 'Normal';
    
    document.getElementById('m-pres').textContent = u.presion;
  }

  document.getElementById('m-total').textContent = d.total || 0;
  document.getElementById('m-offset').textContent = d.batch ?? '—';
  
  if (d.lat_prom !== undefined) {
    document.getElementById('m-lat').textContent = d.lat_prom;
    document.getElementById('m-lat-t').textContent = d.lat_prom > 5000 ? 'Lenta' : 'Óptima';
  }

  if (monitoreoModo === 'vivo') {
    if (d.heatmap) {
      d.heatmap.forEach(h => { if(hmCells[h.h]) hmCells[h.h].style.background = tempColor(h.temp); });
    }
    // Update Line Charts
    if (d.serie) updLine(cTempLine, d.serie);
    if (d.presion_serie) updLine(cPresLine, d.presion_serie);
  }

  if (d.log && d.log.length) {
    document.getElementById('log-box').innerHTML = d.log.map(e =>
      `<div class="log-row">
        <span class="log-ts">${e.ts}</span>
        <span style="color:var(--green)"><i class="fa-solid fa-circle" style="font-size:8px"></i></span>
        <span style="color:var(--amber); font-weight:500;">${e.temp}°C</span>
        <span style="color:var(--blue); font-weight:500;">${e.hum}%</span>
        <span style="color:var(--purple); font-weight:500;">${e.viento}m/s</span>
        <span class="log-desc" style="color:var(--text)"><i class="fa-solid fa-info-circle"></i> ${e.desc}</span>
      </div>`
    ).join('');
  }
}

const es = new EventSource('/stream');
es.onmessage = ev => { try { aplicar(JSON.parse(ev.data)); } catch(e) {} };
es.onerror = () => { document.getElementById('badge-txt').textContent = 'Reconectando...'; };
fetch('/api/estado').then(r=>r.json()).then(aplicar).catch(()=>{});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("\n" + "="*50)
    print("  Dashboard Weather Juliaca")
    print("  http://localhost:5000")
    print("  Actualización: en tiempo real vía SSE")
    print("="*50 + "\n")
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
