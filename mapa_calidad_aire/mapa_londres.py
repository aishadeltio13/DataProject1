import os
import psycopg2
from datetime import datetime
from dash import Dash, html, dcc, callback, Output, Input
import plotly.graph_objects as go

# =============================================================================
# CONFIGURATION
# =============================================================================

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

LONDON_LAT = 51.5074
LONDON_LON = -0.1278
UPDATE_INTERVAL_MS = 30 * 60 * 1000  # 30 minutes in milliseconds

# AQI color scale (EPA standard)
AQI_COLORSCALE = [
    [0, "#00E400"],
    [0.2, "#FFFF00"],
    [0.4, "#FF7E00"],
    [0.6, "#FF0000"],
    [0.8, "#8F3F97"],
    [1.0, "#7E0023"]
]

PARAM_COLORS = {
    "pm25": "#E74C3C",
    "pm10": "#3498DB",
    "no2": "#2ECC71",
    "o3": "#9B59B6"
}

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def wait_for_database(max_retries=30, retry_interval=2):
    """Wait for PostgreSQL database to be ready before proceeding."""
    import time
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS
            )
            conn.close()
            print(f"[DB] Connected to PostgreSQL database successfully.")
            return True
        except Exception as e:
            print(f"[DB] Attempt {attempt + 1}/{max_retries}: Database not ready - {e}")
            time.sleep(retry_interval)
    print("[DB] Could not connect to database after maximum retries.")
    return False


def get_aqi_color(value):
    """Returns category (Spanish for UI) and color based on AQI value."""
    if value is None or value == "-":
        return "Sin datos", "#808080"

    v = int(value) if isinstance(value, str) else value

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


def get_realtime_stations():
    """Fetch latest realtime stations from int__aqi_calculations."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
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
        cur.close()
        conn.close()

        stations = []
        for r in rows:
            _, color = get_aqi_color(r[4])
            stations.append({
                "uid": r[0],
                "name": r[1],
                "lat": r[2],
                "lon": r[3],
                "aqi": r[4],
                "category": r[5],
                "color": color
            })
        print(f"[DB] Displaying {len(stations)} stations (latest available update)")
        return stations
    except Exception as err:
        print(f"[DB] Error fetching stations: {err}")
        return []


def get_station_detail(uid):
    """Fetch detailed pollutant data for a specific station."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
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
        cur.close()
        conn.close()

        if not rows:
            return None

        detail = {
            "name": rows[0][0],
            "pm25": "-",
            "no2": "-",
            "o3": "-",
            "pm10": "-"
        }
        max_aqi = 0
        for r in rows:
            detail[r[1]] = round(r[2], 2)
            if r[3] > max_aqi:
                max_aqi = r[3]

        detail["aqi"] = max_aqi
        detail["dominant"] = "Calculado por DBT"
        return detail
    except Exception as err:
        print(f"[DB] Error fetching station detail: {err}")
        return None


def get_historical_data(station_name):
    """Fetch historical data for a station from int__aqi_calculations."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        query = """
            SELECT sensor_date, parameter, aqi_value, data_origin
            FROM int__aqi_calculations
            WHERE station_name ILIKE %s
            AND sensor_date > NOW() - INTERVAL '60 days'
            ORDER BY sensor_date DESC LIMIT 1000
        """
        cur.execute(query, (f"%{station_name}%",))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        historical = []
        for row in rows:
            historical.append({
                "date": row[0].strftime("%Y-%m-%d %H:%M") if row[0] else "",
                "parameter": row[1],
                "value": row[2],
                "source": row[3]
            })
        return historical
    except Exception:
        return []


def get_db_statistics():
    """Fetch overall statistics from int__aqi_calculations."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        query = """
            SELECT data_origin, COUNT(*) as total, MIN(sensor_date),
                   MAX(sensor_date), COUNT(DISTINCT station_uid)
            FROM int__aqi_calculations
            GROUP BY data_origin
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        statistics = {}
        for row in rows:
            statistics[row[0]] = {
                "total": row[1],
                "date_min": row[2].strftime("%Y-%m-%d") if row[2] else "",
                "date_max": row[3].strftime("%Y-%m-%d") if row[3] else "",
                "stations": row[4]
            }
        return statistics
    except Exception:
        return {}


def get_historical_map_data():
    """Fetch aggregated historical data for map display."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        query = """
            SELECT station_uid, station_name, AVG(lat), AVG(lon),
                   AVG(aqi_value), COUNT(*)
            FROM int__aqi_calculations
            WHERE data_origin = 'historical_data' AND parameter = 'pm25'
            AND sensor_date > NOW() - INTERVAL '60 days'
            GROUP BY station_uid, station_name HAVING COUNT(*) > 5
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        stations = []
        for row in rows:
            category, color = get_aqi_color(int(row[4]) if row[4] else 0)
            stations.append({
                "uid": row[0],
                "name": row[1],
                "lat": row[2],
                "lon": row[3],
                "aqi": int(row[4]) if row[4] else 0,
                "record_count": row[5],
                "category": category,
                "color": color
            })
        return stations
    except Exception:
        return []


def get_pollutant_statistics():
    """Fetch pollutant statistics from historical data."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        query = """
            SELECT parameter, AVG(measurement_value), MIN(measurement_value),
                   MAX(measurement_value), COUNT(*)
            FROM int__aqi_calculations
            WHERE data_origin = 'historical_data'
            AND sensor_date > NOW() - INTERVAL '60 days'
            GROUP BY parameter
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {
            r[0]: {
                "avg": round(r[1], 2),
                "min": round(r[2], 2),
                "max": round(r[3], 2),
                "total": r[4]
            } for r in rows
        }
    except Exception:
        return {}


def get_top_stations():
    """Fetch top 10 most polluted stations by PM2.5 average."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
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
        cur.close()
        conn.close()
        return [(row[0], round(row[1], 2)) for row in rows]
    except Exception:
        return []


# =============================================================================
# VISUALIZATION FUNCTIONS
# =============================================================================

def create_realtime_map(stations):
    """Create the realtime air quality map with heatmap and markers."""
    fig = go.Figure()

    if not stations:
        fig.update_layout(
            mapbox=dict(
                style="carto-positron",
                center=dict(lat=LONDON_LAT, lon=LONDON_LON),
                zoom=10
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            height=600,
            title="Sin datos - Verifica conexion a WAQI"
        )
        return fig

    lats = [s["lat"] for s in stations]
    lons = [s["lon"] for s in stations]
    colors = [s["color"] for s in stations]
    names = [s["name"] for s in stations]
    aqis = [s["aqi"] for s in stations]
    uids = [s["uid"] for s in stations]

    # Calculate marker sizes based on AQI
    sizes = []
    for aqi in aqis:
        if aqi == "-":
            sizes.append(15)
        else:
            sizes.append(min(40, 15 + int(aqi) / 8))

    # Create hover text (Spanish for UI)
    hover_texts = []
    for s in stations:
        hover_texts.append(
            f"<b>{s['name']}</b><br>"
            f"AQI: {s['aqi']}<br>"
            f"Estado: {s['category']}<br>"
            f"<i>Click para ver historico</i>"
        )

    # Filter stations with valid AQI for heatmap
    heatmap_lats = []
    heatmap_lons = []
    heatmap_aqis = []
    for s in stations:
        if s["aqi"] != "-":
            heatmap_lats.append(s["lat"])
            heatmap_lons.append(s["lon"])
            heatmap_aqis.append(int(s["aqi"]))

    # Add density heatmap layer
    if heatmap_lats:
        fig.add_trace(go.Densitymapbox(
            lat=heatmap_lats,
            lon=heatmap_lons,
            z=heatmap_aqis,
            radius=35,
            colorscale=AQI_COLORSCALE,
            zmin=0,
            zmax=300,
            opacity=0.5,
            showscale=True,
            colorbar=dict(title="AQI", x=0.01, xanchor="left"),
            hoverinfo="skip"
        ))

    # Add station markers
    fig.add_trace(go.Scattermapbox(
        lat=lats,
        lon=lons,
        mode="markers+text",
        marker=dict(size=sizes, color=colors, opacity=0.9),
        text=[str(a) if a != "-" else "?" for a in aqis],
        textfont=dict(size=9, color="black"),
        hovertext=hover_texts,
        hoverinfo="text",
        customdata=list(zip(uids, names)),
        name="Estaciones"
    ))

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=LONDON_LAT, lon=LONDON_LON),
            zoom=10
        ),
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        title=dict(text="Calidad del Aire en Londres - Tiempo Real", x=0.5),
        showlegend=False
    )

    return fig


def create_historical_chart(historical_data, station_name):
    """Create historical trend chart for a station."""
    fig = go.Figure()

    if not historical_data:
        fig.add_annotation(
            text="No hay datos historicos en PostgreSQL",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        fig.update_layout(height=350, title=f"Historico - {station_name}")
        return fig

    # Group data by parameter and source
    grouped_data = {}
    for h in historical_data:
        param = h["parameter"]
        source = h.get("source", "unknown")
        key = f"{param}_{source}"

        if key not in grouped_data:
            grouped_data[key] = {
                "dates": [],
                "values": [],
                "parameter": param,
                "source": source
            }
        grouped_data[key]["dates"].append(h["date"])
        grouped_data[key]["values"].append(h["value"])

    # Add traces for each parameter/source combination
    for key, data in grouped_data.items():
        param = data["parameter"]
        source = data["source"]

        line_style = "solid" if source == "historical_data" else "dot"
        marker_symbol = "circle" if source == "historical_data" else "diamond"
        series_name = f"{param.upper()} ({source})"

        fig.add_trace(go.Scatter(
            x=data["dates"],
            y=data["values"],
            mode="lines+markers",
            name=series_name,
            line=dict(
                color=PARAM_COLORS.get(param, "#888888"),
                dash=line_style
            ),
            marker=dict(symbol=marker_symbol, size=6)
        ))

    hist_count = sum(1 for h in historical_data if h.get("source") == "historical_data")
    realtime_count = sum(1 for h in historical_data if h.get("source") == "realtime")

    fig.update_layout(
        title=f"Historico - {station_name}<br><sub>Historical: {hist_count} | Realtime: {realtime_count}</sub>",
        xaxis_title="Fecha",
        yaxis_title="Valor (µg/m³ o AQI)",
        height=350,
        template="plotly_white",
        legend=dict(orientation="h", y=1.15, x=0)
    )

    return fig


def create_historical_map(stations):
    """Create map showing historical PM2.5 averages."""
    fig = go.Figure()

    if not stations:
        fig.add_annotation(
            text="No hay datos históricos disponibles",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        fig.update_layout(
            mapbox=dict(
                style="carto-positron",
                center=dict(lat=LONDON_LAT, lon=LONDON_LON),
                zoom=10
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            height=600,
            title="Datos Históricos - Sin información"
        )
        return fig

    lats = [s["lat"] for s in stations]
    lons = [s["lon"] for s in stations]
    colors = [s["color"] for s in stations]
    aqis = [s["aqi"] for s in stations]
    uids = [s["uid"] for s in stations]
    names = [s["name"] for s in stations]
    record_counts = [s["record_count"] for s in stations]

    # Calculate marker sizes based on record count
    sizes = [min(40, 15 + rc / 10) for rc in record_counts]

    # Create hover text (Spanish for UI)
    hover_texts = []
    for s in stations:
        hover_texts.append(
            f"<b>{s['name']}</b><br>"
            f"PM2.5 Promedio: {s['aqi']}<br>"
            f"Estado: {s['category']}<br>"
            f"Registros: {s['record_count']}"
        )

    # Add density heatmap layer
    if lats:
        fig.add_trace(go.Densitymapbox(
            lat=lats,
            lon=lons,
            z=aqis,
            radius=35,
            colorscale=AQI_COLORSCALE,
            zmin=0,
            zmax=300,
            opacity=0.5,
            showscale=True,
            colorbar=dict(title="PM2.5<br>Avg", x=0.01, xanchor="left"),
            hoverinfo="skip"
        ))

    # Add station markers
    fig.add_trace(go.Scattermapbox(
        lat=lats,
        lon=lons,
        mode="markers+text",
        marker=dict(size=sizes, color=colors, opacity=0.9),
        text=[str(a) for a in aqis],
        textfont=dict(size=9, color="black"),
        hovertext=hover_texts,
        hoverinfo="text",
        customdata=list(zip(uids, names)),
        name="Estaciones Históricas"
    ))

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=LONDON_LAT, lon=LONDON_LON),
            zoom=10
        ),
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        title=dict(text="Datos Históricos - Promedio PM2.5 (Últimos 60 días)", x=0.5),
        showlegend=False
    )

    return fig


def create_pollutant_bar_chart(stats):
    """Create bar chart showing pollutant statistics."""
    fig = go.Figure()

    if not stats:
        fig.add_annotation(
            text="No hay estadísticas disponibles",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        return fig

    parameters = list(stats.keys())
    averages = [stats[p]["avg"] for p in parameters]
    maximums = [stats[p]["max"] for p in parameters]

    fig.add_trace(go.Bar(
        x=parameters,
        y=averages,
        name="Promedio",
        marker_color=[PARAM_COLORS.get(p, "#888888") for p in parameters],
        text=[f"{v:.1f}" for v in averages],
        textposition="outside"
    ))

    fig.add_trace(go.Scatter(
        x=parameters,
        y=maximums,
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


def create_top_stations_chart(top_stations):
    """Create horizontal bar chart showing top polluted stations."""
    fig = go.Figure()

    if not top_stations:
        fig.add_annotation(
            text="No hay datos de estaciones",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14)
        )
        return fig

    names = [s[0][:30] for s in top_stations]
    values = [s[1] for s in top_stations]

    # Assign colors based on AQI value
    bar_colors = []
    for v in values:
        if v <= 50:
            bar_colors.append("#00E400")
        elif v <= 100:
            bar_colors.append("#FFFF00")
        elif v <= 150:
            bar_colors.append("#FF7E00")
        else:
            bar_colors.append("#FF0000")

    fig.add_trace(go.Bar(
        y=names,
        x=values,
        orientation="h",
        marker_color=bar_colors,
        text=[f"{v:.1f}" for v in values],
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


# =============================================================================
# DASH APPLICATION
# =============================================================================

app = Dash(__name__)
app.title = "Mapa Calidad Aire - Londres"

app.layout = html.Div([
    # Header with logo and title
    html.Div([
        html.Img(
            src="/assets/Air_Watch.PNG",
            style={"height": "90px", "marginRight": "20px", "verticalAlign": "middle"}
        ),
        html.Span(
            "Mapa de Calidad del Aire - Londres",
            style={"fontSize": "2em", "fontWeight": "bold", "color": "#2c3e50", "verticalAlign": "middle"}
        )
    ], style={"textAlign": "center", "marginTop": "20px", "marginBottom": "10px"}),

    html.Div(
        id="timestamp-text",
        style={"textAlign": "center", "color": "#7f8c8d", "marginBottom": "10px"}
    ),

    # Legend (Spanish for UI)
    html.Div([
        html.Span("Leyenda: ", style={"fontWeight": "bold"}),
        html.Span(" Bueno (0-50) ", style={"backgroundColor": "#00E400", "padding": "3px 8px", "margin": "2px"}),
        html.Span(" Moderado (51-100) ", style={"backgroundColor": "#FFFF00", "padding": "3px 8px", "margin": "2px"}),
        html.Span(" Sensibles (101-150) ", style={"backgroundColor": "#FF7E00", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Insalubre (151-200) ", style={"backgroundColor": "#FF0000", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Muy Insalubre (201-300) ", style={"backgroundColor": "#8F3F97", "padding": "3px 8px", "margin": "2px", "color": "white"}),
        html.Span(" Peligroso (301+) ", style={"backgroundColor": "#7E0023", "padding": "3px 8px", "margin": "2px", "color": "white"}),
    ], style={
        "textAlign": "center",
        "marginBottom": "15px",
        "padding": "10px",
        "backgroundColor": "#f8f9fa",
        "borderRadius": "10px"
    }),

    html.Div(id="stats-panel", style={"textAlign": "center", "marginBottom": "20px"}),

    html.H2(
        "📡 Tiempo Real (WAQI)",
        style={"color": "#2c3e50", "marginTop": "30px", "marginBottom": "15px"}
    ),

    html.Div([
        html.Div([
            html.H4("🗺️ Mapa en Tiempo Real", style={"color": "#34495e", "marginBottom": "10px", "textAlign": "center"}),
            dcc.Graph(id="realtime-map", config={"scrollZoom": True})
        ], style={"width": "68%", "display": "inline-block", "verticalAlign": "top"}),

        html.Div([
            html.Div(
                id="info-panel",
                children=[
                    html.H3("Selecciona una estacion"),
                    html.P("Haz click en un marcador para ver detalles y el historico.")
                ],
                style={
                    "padding": "15px",
                    "backgroundColor": "#f8f9fa",
                    "borderRadius": "10px",
                    "marginBottom": "15px"
                }
            ),
            html.H4("📈 Historial de Estación", style={"color": "#34495e", "marginBottom": "10px", "textAlign": "center"}),
            dcc.Graph(id="historical-chart")
        ], style={"width": "30%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "2%"})
    ]),

    html.Hr(style={"marginTop": "40px", "marginBottom": "40px"}),

    html.H2(
        "📊 Datos Históricos (OpenAQ)",
        style={"color": "#2c3e50", "marginBottom": "15px"}
    ),

    html.Div([
        html.H4("🗺️ Promedios de Contaminación por Estación (Últimos 30 días)", style={"color": "#34495e", "marginBottom": "10px", "textAlign": "center"}),
        dcc.Graph(id="historical-map", config={"scrollZoom": True})
    ], style={"marginBottom": "30px"}),

    html.H2(
        "📈 Análisis de Datos Históricos",
        style={"color": "#2c3e50", "marginBottom": "15px"}
    ),

    html.Div([
        html.Div([
            html.H4("🧪 Niveles Promedio por Contaminante", style={"color": "#34495e", "marginBottom": "10px", "textAlign": "center"}),
            dcc.Graph(id="pollutant-chart")
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top"}),

        html.Div([
            html.H4("🏭 Top 10 Estaciones más Contaminadas", style={"color": "#34495e", "marginBottom": "10px", "textAlign": "center"}),
            dcc.Graph(id="top-stations-chart")
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "4%"})
    ]),

    dcc.Interval(id="refresh-interval", interval=UPDATE_INTERVAL_MS, n_intervals=0)
], style={
    "fontFamily": "Arial, sans-serif",
    "padding": "20px",
    "maxWidth": "1400px",
    "margin": "0 auto"
})


# =============================================================================
# CALLBACKS
# =============================================================================

@callback(
    [Output("realtime-map", "figure"),
     Output("timestamp-text", "children"),
     Output("stats-panel", "children"),
     Output("historical-map", "figure"),
     Output("pollutant-chart", "figure"),
     Output("top-stations-chart", "figure")],
    Input("refresh-interval", "n_intervals")
)
def update_dashboard(n):
    """Main callback to update all dashboard components."""
    stations = get_realtime_stations()
    realtime_fig = create_realtime_map(stations)

    timestamp = datetime.now().strftime("%H:%M:%S - %d/%m/%Y")
    timestamp_text = f"Datos de api.waqi.info | Actualizado: {timestamp} | Proxima actualizacion en 30 min"

    # Count stations by category (compare with English values from DB, display Spanish in UI)
    total = len(stations)
    good_count = sum(1 for s in stations if s["category"] == "Good")
    moderate_count = sum(1 for s in stations if s["category"] == "Moderate")
    sensitive_count = sum(1 for s in stations if s["category"] == "Unhealthy for Sensitive Groups")
    unhealthy_count = sum(1 for s in stations if s["category"] in ["Unhealthy", "Very Unhealthy", "Hazardous"])

    db_stats = get_db_statistics()

    # Build stats panel (Spanish for UI)
    stats_components = [
        html.Div([
            html.H4("📡 Tiempo Real (WAQI)", style={"margin": "5px 0"}),
            html.Span(f"Total: {total} estaciones | ", style={"fontWeight": "bold"}),
            html.Span(f"Bueno: {good_count} | ", style={"color": "#00E400", "fontWeight": "bold"}),
            html.Span(f"Moderado: {moderate_count} | ", style={"color": "#f39c12", "fontWeight": "bold"}),
            html.Span(f"Sensibles: {sensitive_count} | ", style={"color": "#FF7E00", "fontWeight": "bold"}),
            html.Span(f"Insalubre: {unhealthy_count}", style={"color": "#e74c3c", "fontWeight": "bold"})
        ], style={"marginBottom": "15px"}),
    ]

    if "historical_data" in db_stats:
        hist = db_stats["historical_data"]
        stats_components.append(html.Div([
            html.H4("📊 Datos Históricos (OpenAQ)", style={"margin": "5px 0"}),
            html.Span(f"Total: {hist['total']:,} registros | ", style={"fontWeight": "bold"}),
            html.Span(f"Estaciones: {hist['stations']} | ", style={"fontWeight": "bold"}),
            html.Span(f"Desde: {hist['date_min']} ", style={"color": "#7f8c8d"}),
            html.Span(f"hasta {hist['date_max']}", style={"color": "#7f8c8d"})
        ], style={"marginBottom": "15px"}))

    if "realtime" in db_stats:
        rt = db_stats["realtime"]
        stats_components.append(html.Div([
            html.H4("💾 Datos Almacenados (Realtime)", style={"margin": "5px 0"}),
            html.Span(f"Total: {rt['total']:,} registros | ", style={"fontWeight": "bold"}),
            html.Span(f"Estaciones: {rt['stations']} | ", style={"fontWeight": "bold"}),
            html.Span(f"Desde: {rt['date_min']} ", style={"color": "#7f8c8d"}),
            html.Span(f"hasta {rt['date_max']}", style={"color": "#7f8c8d"})
        ]))

    stats_panel = html.Div(stats_components, style={
        "backgroundColor": "#f8f9fa",
        "padding": "15px",
        "borderRadius": "10px"
    })

    historical_stations = get_historical_map_data()
    historical_fig = create_historical_map(historical_stations)

    pollutant_stats = get_pollutant_statistics()
    pollutant_fig = create_pollutant_bar_chart(pollutant_stats)

    top_stations = get_top_stations()
    top_fig = create_top_stations_chart(top_stations)

    return realtime_fig, timestamp_text, stats_panel, historical_fig, pollutant_fig, top_fig


@callback(
    [Output("info-panel", "children"),
     Output("historical-chart", "figure")],
    Input("realtime-map", "clickData"),
    prevent_initial_call=True
)
def on_station_click(click_data):
    """Callback triggered when user clicks on a station marker."""
    if not click_data:
        return [html.H3("Selecciona una estacion")], go.Figure()

    point = click_data["points"][0]
    custom_data = point.get("customdata", [None, "Desconocida"])
    uid = custom_data[0]
    name = custom_data[1]

    detail = get_station_detail(uid) if uid else None
    historical = get_historical_data(name)

    hist_count = sum(1 for h in historical if h.get("source") == "historical_data")
    realtime_count = sum(1 for h in historical if h.get("source") == "realtime")

    # Build info panel (Spanish for UI)
    if detail:
        category, color = get_aqi_color(detail["aqi"])
        panel = [
            html.H3(detail["name"] or name, style={"color": "#2c3e50"}),
            html.Div([
                html.Span("AQI: "),
                html.Span(str(detail["aqi"]), style={
                    "backgroundColor": color,
                    "padding": "5px 15px",
                    "borderRadius": "5px",
                    "color": "white" if color not in ["#FFFF00", "#00E400"] else "black",
                    "fontWeight": "bold"
                }),
                html.Span(f" ({category})")
            ], style={"marginBottom": "10px"}),
            html.Hr(),
            html.H4("Contaminantes (Tiempo Real)", style={"fontSize": "1em", "marginTop": "10px"}),
            html.P([html.B("PM2.5: "), f"{detail['pm25']}"]),
            html.P([html.B("PM10: "), f"{detail['pm10']}"]),
            html.P([html.B("NO2: "), f"{detail['no2']}"]),
            html.P([html.B("O3: "), f"{detail['o3']}"]),
            html.Hr(),
            html.P(f"Dominante: {detail['dominant'].upper()}" if detail['dominant'] else ""),
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
            html.P(f"Total: {len(historical)} registros", style={"fontSize": "0.9em", "color": "#7f8c8d"})
        ]
    else:
        panel = [
            html.H3(name),
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
            html.P(f"Total: {len(historical)} registros")
        ]

    fig = create_historical_chart(historical, name)

    return panel, fig


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("LONDON AIR QUALITY MAP")
    print("=" * 60)
    print()
    print("DATA SOURCES:")
    print("  - Realtime: WAQI API (api.waqi.info)")
    print("  - Historical: DBT (int__aqi_calculations)")
    print()

    # Wait for database to be ready before starting
    if not wait_for_database():
        print("[ERROR] Could not connect to database. Exiting.")
        exit(1)

    print()
    print("Open in browser: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    app.run(debug=False, host="0.0.0.0", port=8050)
