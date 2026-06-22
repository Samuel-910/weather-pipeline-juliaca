"""
Dashboard Weather Juliaca — Tiempo Real
Usa Server-Sent Events (SSE): el navegador se actualiza
CADA VEZ que Kafka recibe un nuevo evento de Spark.
Abrir: http://localhost:5000
"""

import os
import json
import time
import queue
import threading
from datetime import datetime
from collections import defaultdict, deque
from flask import Flask, render_template_string, jsonify, Response, stream_with_context
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
            print(f"Kafka conectado → {BROKER} | tópico: {TOPIC}")

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
  --green: #10b981;
  --red: #ef4444;
  --bar-bg: #334155;
  --card-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3), 0 4px 6px -4px rgba(0, 0, 0, 0.3);
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
  --green: #059669;
  --red: #dc2626;
  --bar-bg: #e2e8f0;
  --card-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.025);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); padding: 20px; transition: background 0.3s, color 0.3s; }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding: 10px; }
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
@media(max-width:800px){ .r2 { grid-template-columns: 1fr; } }

.panel { background: var(--bg2); border: 1px solid var(--border); border-radius: 16px; padding: 20px; box-shadow: var(--card-shadow); }
.panel h2 { font-size: 13px; font-weight: 600; color: var(--text-muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 15px; display: flex; align-items: center; gap: 8px; }

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

<!-- Row: Gauges (Doughnut charts) -->
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

<script>
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

  if (d.heatmap) {
    d.heatmap.forEach(h => { hmCells[h.h].style.background = tempColor(h.temp); });
  }

  // Update Line Charts
  if (d.serie) updLine(cTempLine, d.serie);
  if (d.presion_serie) updLine(cPresLine, d.presion_serie);

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
