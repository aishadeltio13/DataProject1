import os
import requests
import psycopg2
from datetime import datetime
from dash import Dash, html, dcc, callback, Output, Input
import plotly.graph_objects as go

# =============================================================================
# CONFIGURATION
# =============================================================================

WAQI_TOKEN = os.getenv("WAQI_API_TOKEN")
LONDON_BOUNDS = os.getenv("LONDON_BOUNDS")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

LONDON_LAT = 51.5074
LONDON_LON = -0.1278
UPDATE_INTERVAL_30_MIN = 30 * 60 * 1000

# =============================================================================
# FUNCTIONS
# =============================================================================

def color_aqi(valor):
    if valor is None or valor == "-":
        return "Sin datos", "#808080"

    v = int(valor) if isinstance(valor, str) else valor

    if v <= 50:
        return "Bueno", "#00E400"
    elif v <= 100:
        return "Moderado", "#FFFF00"
    elif v <= 150:
        return "Sensibles", "#FF7E00"
    elif v <= 200:
        return "Insalubre", "#FF0000"
    elif v <= 300:
        return "Muy Insalubre", "#8F3F97"
    else:
        return "Peligroso", "#7E0023"

def obtener_estaciones_waqi():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        query = """
            SELECT DISTINCT ON (station_uid)
                station_uid, station_name, lat, lon, aqi_value, aqi_category
            FROM int__aqi_calculations
            WHERE data_origin = 'realtime'
            ORDER BY station_uid, sensor_date DESC
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()
        
        estaciones = []
        for r in rows:
            _, col = color_aqi(r[4]) 
            estaciones.append({
                "uid": r[0], "nombre": r[1], "lat": r[2], "lon": r[3],
                "aqi": r[4], "categoria": r[5], "color": col
            })
        print(f"[DB] Mostrando {len(estaciones)} estaciones (última actualización disponible)")
        return estaciones
    except Exception as err:
        print(f"Error en DB (estaciones): {err}")
        return []

def obtener_detalle_waqi(uid):
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()

        query = """
            WITH last_date AS (
                SELECT MAX(sensor_date) as max_date 
                FROM int__aqi_calculations 
                WHERE station_uid = %s AND data_origin = 'realtime'
            )
            SELECT station_name, parameter, measurement_value, aqi_value
            FROM int__aqi_calculations, last_date
            WHERE station_uid = %s 
            AND sensor_date = last_date.max_date
            AND data_origin = 'realtime'
        """
        cur.execute(query, (uid, uid))
        rows = cur.fetchall()
        cur.close(); conn.close()
        
        if not rows: return None
        detalle = {"nombre": rows[0][0], "pm25": "-", "no2": "-", "o3": "-", "pm10": "-"}
        aqi_max = 0
        for r in rows:
            detalle[r[1]] = round(r[2], 2)
            if r[3] > aqi_max: aqi_max = r[3]
        
        detalle["aqi"] = aqi_max
        detalle["dominante"] = "Calculado por dbt"
        return detalle
    except Exception as err:
        print(f"Error en DB (detalle): {err}")
        return None

def obtener_historico_db(nombre_estacion):
    """Read from int__aqi_calculations"""
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        # dbt: source -> data_origin | value -> aqi_value
        query = """
            SELECT sensor_date, parameter, aqi_value, data_origin
            FROM int__aqi_calculations
            WHERE station_name ILIKE %s
            AND sensor_date > NOW() - INTERVAL '60 days'
            ORDER BY sensor_date DESC LIMIT 1000
        """
        cur.execute(query, (f"%{nombre_estacion}%",))
        rows = cur.fetchall()
        cur.close(); conn.close()
        historico = []
        for row in rows:
            historico.append({
                "fecha": row[0].strftime("%Y-%m-%d %H:%M") if row[0] else "",
                "parameter": row[1], "value": row[2], "source": row[3]
            })
        return historico
    except Exception: return []

def obtener_estadisticas_db():
    """Read from int__aqi_calculations"""
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        query = """
            SELECT data_origin, COUNT(*) as total, MIN(sensor_date), MAX(sensor_date), COUNT(DISTINCT station_uid)
            FROM int__aqi_calculations
            GROUP BY data_origin
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()
        estadisticas = {}
        for row in rows:
            estadisticas[row[0]] = {
                "total": row[1], "fecha_min": row[2].strftime("%Y-%m-%d") if row[2] else "",
                "fecha_max": row[3].strftime("%Y-%m-%d") if row[3] else "", "estaciones": row[4]
            }
        return estadisticas
    except Exception: return {}

def obtener_datos_historicos_mapa():
    """Read from int__aqi_calculations"""
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        query = """
            SELECT station_uid, station_name, AVG(lat), AVG(lon), AVG(aqi_value), COUNT(*)
            FROM int__aqi_calculations
            WHERE data_origin = 'historical_data' AND parameter = 'pm25'
            AND sensor_date > NOW() - INTERVAL '60 days'
            GROUP BY station_uid, station_name HAVING COUNT(*) > 5
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()
        estaciones = []
        for row in rows:
            cat, col = color_aqi(int(row[4]) if row[4] else 0)
            estaciones.append({
                "uid": row[0], "nombre": row[1], "lat": row[2], "lon": row[3],
                "aqi": int(row[4]) if row[4] else 0, "num_registros": row[5],
                "categoria": cat, "color": col
            })
        return estaciones
    except Exception: return []

def obtener_estadisticas_contaminantes():
    """Read from de int__aqi_calculations"""
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        query = """
            SELECT parameter, AVG(measurement_value), MIN(measurement_value), MAX(measurement_value), COUNT(*)
            FROM int__aqi_calculations
            WHERE data_origin = 'historical_data' AND sensor_date > NOW() - INTERVAL '60 days'
            GROUP BY parameter
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {r[0]: {"avg": round(r[1], 2), "min": round(r[2], 2), "max": round(r[3], 2), "total": r[4]} for r in rows}
    except Exception: return {}


def obtener_top_estaciones():
    """Read from de int__aqi_calculations"""
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        query = """
            SELECT station_name, AVG(aqi_value) as avg_aqi
            FROM int__aqi_calculations
            WHERE data_origin = 'historical_data' AND parameter = 'pm25'
            AND sensor_date > NOW() - INTERVAL '60 days'
            GROUP BY station_name ORDER BY avg_aqi DESC LIMIT 10
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [(row[0], round(row[1], 2)) for row in rows]
    except Exception: return []




def crear_mapa(estaciones):
    fig = go.Figure()

    if not estaciones:
        fig.update_layout(
            mapbox=dict(style="carto-positron", center=dict(lat=LONDON_LAT, lon=LONDON_LON), zoom=10),
            margin=dict(l=0, r=0, t=40, b=0),
            height=600,
            title="Sin datos - Verifica conexion a WAQI"
        )
        return fig

    lats = [e["lat"] for e in estaciones]
    lons = [e["lon"] for e in estaciones]
    colores = [e["color"] for e in estaciones]
    nombres = [e["nombre"] for e in estaciones]
    aqis = [e["aqi"] for e in estaciones]
    uids = [e["uid"] for e in estaciones]

    tamanos = []
    for aqi in aqis:
        if aqi == "-":
            tamanos.append(15)
        else:
            tamanos.append(min(40, 15 + int(aqi) / 8))

    hovers = []
    for e in estaciones:
        hovers.append(
            f"<b>{e['nombre']}</b><br>"
            f"AQI: {e['aqi']}<br>"
            f"Estado: {e['categoria']}<br>"
            f"<i>Click para ver historico</i>"
        )

    lats_h = []
    lons_h = []
    aqis_h = []
    for e in estaciones:
        if e["aqi"] != "-":
            lats_h.append(e["lat"])
            lons_h.append(e["lon"])
            aqis_h.append(int(e["aqi"]))

    if lats_h:
        fig.add_trace(go.Densitymapbox(
            lat=lats_h,
            lon=lons_h,
            z=aqis_h,
            radius=35,
            colorscale=[
                [0, "#00E400"],
                [0.2, "#FFFF00"],
                [0.4, "#FF7E00"],
                [0.6, "#FF0000"],
                [0.8, "#8F3F97"],
                [1.0, "#7E0023"]
            ],
            zmin=0,
            zmax=300,
            opacity=0.5,
            showscale=True,
            colorbar=dict(title="AQI", x=0.01, xanchor="left"),
            hoverinfo="skip"
        ))

    fig.add_trace(go.Scattermapbox(
        lat=lats,
        lon=lons,
        mode="markers+text",
        marker=dict(size=tamanos, color=colores, opacity=0.9),
        text=[str(a) if a != "-" else "?" for a in aqis],
        textfont=dict(size=9, color="black"),
        hovertext=hovers,
        hoverinfo="text",
        customdata=list(zip(uids, nombres)),
        name="Estaciones"
    ))

    fig.update_layout(
        mapbox=dict(style="carto-positron", center=dict(lat=LONDON_LAT, lon=LONDON_LON), zoom=10),
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        title=dict(text="Calidad del Aire en Londres - Tiempo Real", x=0.5),
        showlegend=False
    )

    return fig

def crear_grafico_historico(historico, nombre_estacion):
    fig = go.Figure()

    if not historico:
        fig.add_annotation(
            text="No hay datos historicos en PostgreSQL",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        fig.update_layout(height=350, title=f"Historico - {nombre_estacion}")
        return fig

    por_parametro = {}
    for h in historico:
        p = h["parameter"]
        source = h.get("source", "unknown")
        key = f"{p}_{source}"

        if key not in por_parametro:
            por_parametro[key] = {
                "fechas": [],
                "valores": [],
                "parameter": p,
                "source": source
            }
        por_parametro[key]["fechas"].append(h["fecha"])
        por_parametro[key]["valores"].append(h["value"])

    colores_param = {
        "pm25": "#E74C3C",
        "pm10": "#3498DB",
        "no2": "#2ECC71",
        "o3": "#9B59B6"
    }

    for key, datos in por_parametro.items():
        param = datos["parameter"]
        source = datos["source"]

        line_style = "solid" if source == "historical_data" else "dot"
        marker_symbol = "circle" if source == "historical_data" else "diamond"
        nombre_serie = f"{param.upper()} ({source})"

        fig.add_trace(go.Scatter(
            x=datos["fechas"],
            y=datos["valores"],
            mode="lines+markers",
            name=nombre_serie,
            line=dict(
                color=colores_param.get(param, "#888888"),
                dash=line_style
            ),
            marker=dict(symbol=marker_symbol, size=6)
        ))

    hist_count = sum(1 for h in historico if h.get("source") == "historical_data")
    realtime_count = sum(1 for h in historico if h.get("source") == "realtime")

    fig.update_layout(
        title=f"Historico - {nombre_estacion}<br><sub>Historical: {hist_count} | Realtime: {realtime_count}</sub>",
        xaxis_title="Fecha",
        yaxis_title="Valor (µg/m³ o AQI)",
        height=350,
        template="plotly_white",
        legend=dict(orientation="h", y=1.15, x=0)
    )

    return fig

def crear_mapa_historico(estaciones):
    fig = go.Figure()

    if not estaciones:
        fig.add_annotation(
            text="No hay datos históricos disponibles",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        fig.update_layout(
            mapbox=dict(style="carto-positron", center=dict(lat=LONDON_LAT, lon=LONDON_LON), zoom=10),
            margin=dict(l=0, r=0, t=40, b=0),
            height=600,
            title="Datos Históricos - Sin información"
        )
        return fig

    lats = [e["lat"] for e in estaciones]
    lons = [e["lon"] for e in estaciones]
    colores = [e["color"] for e in estaciones]
    nombres = [e["nombre"] for e in estaciones]
    aqis = [e["aqi"] for e in estaciones]
    uids = [e["uid"] for e in estaciones]
    num_registros = [e["num_registros"] for e in estaciones]

    tamanos = []
    for nr in num_registros:
        tamanos.append(min(40, 15 + nr / 10))

    hovers = []
    for e in estaciones:
        hovers.append(
            f"<b>{e['nombre']}</b><br>"
            f"PM2.5 Promedio: {e['aqi']}<br>"
            f"Estado: {e['categoria']}<br>"
            f"Registros: {e['num_registros']}"
        )

    lats_h = [e["lat"] for e in estaciones]
    lons_h = [e["lon"] for e in estaciones]
    aqis_h = [e["aqi"] for e in estaciones]

    if lats_h:
        fig.add_trace(go.Densitymapbox(
            lat=lats_h,
            lon=lons_h,
            z=aqis_h,
            radius=35,
            colorscale=[
                [0, "#00E400"],
                [0.2, "#FFFF00"],
                [0.4, "#FF7E00"],
                [0.6, "#FF0000"],
                [0.8, "#8F3F97"],
                [1.0, "#7E0023"]
            ],
            zmin=0,
            zmax=300,
            opacity=0.5,
            showscale=True,
            colorbar=dict(title="PM2.5<br>Avg", x=0.01, xanchor="left"),
            hoverinfo="skip"
        ))

    fig.add_trace(go.Scattermapbox(
        lat=lats,
        lon=lons,
        mode="markers+text",
        marker=dict(size=tamanos, color=colores, opacity=0.9),
        text=[str(a) for a in aqis],
        textfont=dict(size=9, color="black"),
        hovertext=hovers,
        hoverinfo="text",
        customdata=list(zip(uids, nombres)),
        name="Estaciones Históricas"
    ))

    fig.update_layout(
        mapbox=dict(style="carto-positron", center=dict(lat=LONDON_LAT, lon=LONDON_LON), zoom=10),
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        title=dict(text="Datos Históricos - Promedio PM2.5 (Últimos 60 días)", x=0.5),
        showlegend=False
    )

    return fig

def crear_grafico_barras_contaminantes(stats):
    fig = go.Figure()

    if not stats:
        fig.add_annotation(
            text="No hay estadísticas disponibles",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        return fig

    colores = {
        "pm25": "#E74C3C",
        "pm10": "#3498DB",
        "no2": "#2ECC71",
        "o3": "#9B59B6"
    }

    parametros = list(stats.keys())
    promedios = [stats[p]["avg"] for p in parametros]
    maximos = [stats[p]["max"] for p in parametros]

    fig.add_trace(go.Bar(
        x=parametros,
        y=promedios,
        name="Promedio",
        marker_color=[colores.get(p, "#888888") for p in parametros],
        text=[f"{v:.1f}" for v in promedios],
        textposition="outside"
    ))

    fig.add_trace(go.Scatter(
        x=parametros,
        y=maximos,
        mode="lines+markers",
        name="Máximo",
        line=dict(color="red", dash="dash"),
        marker=dict(size=10, symbol="diamond")
    ))

    fig.update_layout(
        title="Estadísticas de Contaminantes (Histórico - 60 días)",
        xaxis_title="Contaminante",
        yaxis_title="Valor (µg/m³)",
        height=400,
        template="plotly_white",
        legend=dict(x=0.8, y=1)
    )

    return fig


def crear_grafico_top_estaciones(top_estaciones):
    fig = go.Figure()

    if not top_estaciones:
        fig.add_annotation(
            text="No hay datos de estaciones",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        return fig

    nombres = [e[0][:30] for e in top_estaciones]
    valores = [e[1] for e in top_estaciones]

    colors = []
    for v in valores:
        if v <= 50:
            colors.append("#00E400")
        elif v <= 100:
            colors.append("#FFFF00")
        elif v <= 150:
            colors.append("#FF7E00")
        else:
            colors.append("#FF0000")

    fig.add_trace(go.Bar(
        y=nombres,
        x=valores,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}" for v in valores],
        textposition="outside"
    ))

    fig.update_layout(
        title="Top 10 Estaciones - Mayor Promedio PM2.5 (Histórico)",
        xaxis_title="PM2.5 Promedio (µg/m³)",
        yaxis_title="",
        height=500,
        template="plotly_white",
        yaxis=dict(autorange="reversed")
    )

    return fig

# APLICACION DASH


app = Dash(__name__)
app.title = "Mapa Calidad Aire - Londres"

app.layout = html.Div([
    html.H1("Mapa de Calidad del Aire - Londres",
            style={"textAlign": "center", "color": "#2c3e50", "marginTop": "20px"}),

    html.Div(id="texto-hora", style={"textAlign": "center", "color": "#7f8c8d", "marginBottom": "10px"}),

    html.Div([
        html.Span("Leyenda: ", style={"fontWeight": "bold"}),
        html.Span(" Bueno (0-50) ", style={"backgroundColor": "#00E400", "padding": "3px 8px", "margin": "2px"}),
        html.Span(" Moderado (51-100) ", style={"backgroundColor": "#FFFF00", "padding": "3px 8px", "margin": "2px"}),
        html.Span(" Sensibles (101-150) ", style={"backgroundColor": "#FF7E00", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Insalubre (151-200) ", style={"backgroundColor": "#FF0000", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Muy Insalubre (201-300) ", style={"backgroundColor": "#8F3F97", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Peligroso (301+) ", style={"backgroundColor": "#7E0023", "padding": "3px 8px", "margin": "2px", "color": "white"}),
    ], style={"textAlign": "center", "marginBottom": "15px", "padding": "10px", "backgroundColor": "#f8f9fa", "borderRadius": "10px"}),

    html.Div(id="stats", style={"textAlign": "center", "marginBottom": "20px"}),

    html.H2("📡 Tiempo Real (WAQI)", style={"color": "#2c3e50", "marginTop": "30px", "marginBottom": "15px"}),

    html.Div([
        html.Div([
            dcc.Graph(id="mapa", config={"scrollZoom": True})
        ], style={"width": "68%", "display": "inline-block", "verticalAlign": "top"}),

        html.Div([
            html.Div(id="panel-info", children=[
                html.H3("Selecciona una estacion"),
                html.P("Haz click en un marcador para ver detalles y el historico.")
            ], style={"padding": "15px", "backgroundColor": "#f8f9fa", "borderRadius": "10px", "marginBottom": "15px"}),

            dcc.Graph(id="grafico-hist")
        ], style={"width": "30%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "2%"})
    ]),

    html.Hr(style={"marginTop": "40px", "marginBottom": "40px"}),

    html.H2("📊 Datos Históricos (OpenAQ)", style={"color": "#2c3e50", "marginBottom": "15px"}),

    html.Div([
        dcc.Graph(id="mapa-historico", config={"scrollZoom": True})
    ], style={"marginBottom": "30px"}),

    html.H2("📈 Análisis de Datos Históricos", style={"color": "#2c3e50", "marginBottom": "15px"}),

    html.Div([
        html.Div([
            dcc.Graph(id="grafico-contaminantes")
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top"}),

        html.Div([
            dcc.Graph(id="grafico-top-estaciones")
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "4%"})
    ]),

    dcc.Interval(id="intervalo", interval=UPDATE_INTERVAL_30_MIN, n_intervals=0)
], style={"fontFamily": "Arial, sans-serif", "padding": "20px", "maxWidth": "1400px", "margin": "0 auto"})

# CALLBACKS

@callback(
    [Output("mapa", "figure"),
     Output("texto-hora", "children"),
     Output("stats", "children"),
     Output("mapa-historico", "figure"),
     Output("grafico-contaminantes", "figure"),
     Output("grafico-top-estaciones", "figure")],
    Input("intervalo", "n_intervals")
)
def actualizar_datos(n):
    estaciones = obtener_estaciones_waqi()
    fig_realtime = crear_mapa(estaciones)

    hora = datetime.now().strftime("%H:%M:%S - %d/%m/%Y")
    texto = f"Datos de api.waqi.info | Actualizado: {hora} | Proxima actualizacion en 30 min"

    total = len(estaciones)
    bueno = sum(1 for e in estaciones if e["categoria"] == "Bueno")
    moderado = sum(1 for e in estaciones if e["categoria"] == "Moderado")
    malo = sum(1 for e in estaciones if e["categoria"] in ["Insalubre", "Muy Insalubre", "Peligroso", "Sensibles"])

    estadisticas_db = obtener_estadisticas_db()

    stats_componentes = [
        html.Div([
            html.H4("📡 Tiempo Real (WAQI)", style={"margin": "5px 0"}),
            html.Span(f"Total: {total} estaciones | ", style={"fontWeight": "bold"}),
            html.Span(f"Bueno: {bueno} | ", style={"color": "#00E400", "fontWeight": "bold"}),
            html.Span(f"Moderado: {moderado} | ", style={"color": "#f39c12", "fontWeight": "bold"}),
            html.Span(f"Malo: {malo}", style={"color": "#e74c3c", "fontWeight": "bold"})
        ], style={"marginBottom": "15px"}),
    ]

    if "historical_data" in estadisticas_db:
        hist_stats = estadisticas_db["historical_data"]
        stats_componentes.append(html.Div([
            html.H4("📊 Datos Históricos (OpenAQ)", style={"margin": "5px 0"}),
            html.Span(f"Total: {hist_stats['total']:,} registros | ", style={"fontWeight": "bold"}),
            html.Span(f"Estaciones: {hist_stats['estaciones']} | ", style={"fontWeight": "bold"}),
            html.Span(f"Desde: {hist_stats['fecha_min']} ", style={"color": "#7f8c8d"}),
            html.Span(f"hasta {hist_stats['fecha_max']}", style={"color": "#7f8c8d"})
        ], style={"marginBottom": "15px"}))

    if "realtime" in estadisticas_db:
        rt_stats = estadisticas_db["realtime"]
        stats_componentes.append(html.Div([
            html.H4("💾 Datos Almacenados (Realtime)", style={"margin": "5px 0"}),
            html.Span(f"Total: {rt_stats['total']:,} registros | ", style={"fontWeight": "bold"}),
            html.Span(f"Estaciones: {rt_stats['estaciones']} | ", style={"fontWeight": "bold"}),
            html.Span(f"Desde: {rt_stats['fecha_min']} ", style={"color": "#7f8c8d"}),
            html.Span(f"hasta {rt_stats['fecha_max']}", style={"color": "#7f8c8d"})
        ]))

    stats = html.Div(stats_componentes, style={
        "backgroundColor": "#f8f9fa",
        "padding": "15px",
        "borderRadius": "10px"
    })

    estaciones_historicas = obtener_datos_historicos_mapa()
    fig_historico = crear_mapa_historico(estaciones_historicas)

    stats_contaminantes = obtener_estadisticas_contaminantes()
    fig_contaminantes = crear_grafico_barras_contaminantes(stats_contaminantes)

    top_estaciones = obtener_top_estaciones()
    fig_top = crear_grafico_top_estaciones(top_estaciones)

    return fig_realtime, texto, stats, fig_historico, fig_contaminantes, fig_top


@callback(
    [Output("panel-info", "children"),
     Output("grafico-hist", "figure")],
    Input("mapa", "clickData"),
    prevent_initial_call=True
)
def click_estacion(clickData):
    if not clickData:
        return [html.H3("Selecciona una estacion")], go.Figure()

    punto = clickData["points"][0]
    customdata = punto.get("customdata", [None, "Desconocida"])
    uid = customdata[0]
    nombre = customdata[1]

    detalle = obtener_detalle_waqi(uid) if uid else None
    historico = obtener_historico_db(nombre)

    hist_count = sum(1 for h in historico if h.get("source") == "historical_data")
    realtime_count = sum(1 for h in historico if h.get("source") == "realtime")

    if detalle:
        cat, col = color_aqi(detalle["aqi"])
        panel = [
            html.H3(detalle["nombre"] or nombre, style={"color": "#2c3e50"}),
            html.Div([
                html.Span("AQI: "),
                html.Span(str(detalle["aqi"]), style={
                    "backgroundColor": col,
                    "padding": "5px 15px",
                    "borderRadius": "5px",
                    "color": "white" if col not in ["#FFFF00", "#00E400"] else "black",
                    "fontWeight": "bold"
                }),
                html.Span(f" ({cat})")
            ], style={"marginBottom": "10px"}),
            html.Hr(),
            html.H4("Contaminantes (Tiempo Real)", style={"fontSize": "1em", "marginTop": "10px"}),
            html.P([html.B("PM2.5: "), f"{detalle['pm25']}"]),
            html.P([html.B("PM10: "), f"{detalle['pm10']}"]),
            html.P([html.B("NO2: "), f"{detalle['no2']}"]),
            html.P([html.B("O3: "), f"{detalle['o3']}"]),
            html.Hr(),
            html.P(f"Dominante: {detalle['dominante'].upper()}" if detalle['dominante'] else ""),
            html.Hr(),
            html.H4("Base de Datos PostgreSQL", style={"fontSize": "1em", "marginTop": "10px"}),
            html.P([
                html.Span("📊 Históricos (OpenAQ): ", style={"fontWeight": "bold"}),
                html.Span(f"{hist_count} registros", style={"color": "#3498DB"})
            ]),
            html.P([
                html.Span("📡 Realtime (WAQI): ", style={"fontWeight": "bold"}),
                html.Span(f"{realtime_count} registros", style={"color": "#E74C3C"})
            ]),
            html.P(f"Total: {len(historico)} registros", style={"fontSize": "0.9em", "color": "#7f8c8d"})
        ]
    else:
        panel = [
            html.H3(nombre),
            html.P("No se pudo obtener detalle de WAQI"),
            html.Hr(),
            html.H4("Base de Datos PostgreSQL", style={"fontSize": "1em"}),
            html.P([
                html.Span("📊 Históricos: ", style={"fontWeight": "bold"}),
                html.Span(f"{hist_count} registros", style={"color": "#3498DB"})
            ]),
            html.P([
                html.Span("📡 Realtime: ", style={"fontWeight": "bold"}),
                html.Span(f"{realtime_count} registros", style={"color": "#E74C3C"})
            ]),
            html.P(f"Total: {len(historico)} registros")
        ]

    fig = crear_grafico_historico(historico, nombre)

    return panel, fig

# EJECUTAR

if __name__ == "__main__":
    print("=" * 60)
    print("MAPA DE CALIDAD DEL AIRE - LONDRES")
    print("=" * 60)
    print()
    print("FUENTES DE DATOS:")
    print("  - Tiempo real: API WAQI (api.waqi.info)")
    print("  - Historico: PostgreSQL (tabla registroaire)")
    print()
    print("Abre en tu navegador: http://127.0.0.1:8050")
    print("Presiona Ctrl+C para detener")
    print("=" * 60)

    app.run(debug=False, host="0.0.0.0", port=8050)
