# 🌍 AirWatch London
### Sistema de Monitoreo de Calidad del Aire

> Proyecto de ingesta, transformación y visualización de datos de calidad del aire en Londres usando APIs públicas (WAQI y OpenAQ), PostgreSQL, DBT y Docker.

---

## 📋 Descripción del Proyecto

Este proyecto descarga datos de contaminación atmosférica de Londres desde dos fuentes:

- 🔴 **Tiempo real**: API de WAQI (World Air Quality Index) cada 30 minutos
- 📚 **Históricos**: API de OpenAQ con datos desde 2025

Los datos se almacenan en PostgreSQL, se transforman con DBT para calcular el índice AQI, detectar anomalías y generar alertas. Finalmente se visualizan en:

- 🗺️ **Dashboard interactivo** con Plotly/Dash (mapas + gráficos)
- 💬 **Canal de Telegram** que envía alertas automáticas
- 📊 **Grafana** para análisis avanzados

---

## 🏗️ Arquitectura
```
┌─────────────────┐     ┌─────────────────┐
│   WAQI API      │     │   OpenAQ API    │
│  (Realtime)     │     │  (Historical)   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         v                       v
    ┌────────────────────────────────┐
    │      FastAPI (API_DB)          │
    │   (Validación + Inserción)     │
    └────────────┬───────────────────┘
                 │
                 v
         ┌───────────────┐
         │  PostgreSQL   │
         │   (Datos)     │
         └───────┬───────┘
                 │
                 v
         ┌───────────────┐
         │      DBT      │
         │ (Transform)   │
         └───────┬───────┘
                 │
      ┌──────────┴──────────┐
      v                     v
┌─────────────┐      ┌──────────────┐
│ Plotly/Dash │      │   Telegram   │
│  Grafana    │      │   (Alertas)  │
└─────────────┘      └──────────────┘
```

---

## 🚀 Cómo Empezar

### 1️⃣ Prerequisitos

Claves API (gratis):
- 🌐 **WAQI**: https://aqicn.org/data-platform/token/
- 📊 **OpenAQ**: https://explore.openaq.org/api
- 💬 **Telegram Bot**: https://t.me/AirWatch_London

### 2️⃣ Configurar Variables de Entorno

Edita el archivo `.env` con tus propias claves:
```bash
# --- WAQI API CONFIGURATION (REAL TIME) ---
WAQI_API_TOKEN=tu_token_waqi_aqui
LONDON_BOUNDS=51.28,-0.51,51.69,0.33
SCAN_INTERVAL=1800
API_TOKEN_APIDB_REALTIME=pega_la_nueva_llave_aqui

# --- OPENAQ V3 KEY (HISTORICAL) ---
OPENAQ_API_KEY=tu_token_openaq_aqui
OPENAQ_BOUNDS=-0.51,51.28,0.33,51.69
API_TOKEN_APIDB_HISTORICAL=pega_la_nueva_llave_aqui

# --- DATABASE CONFIGURATION ---
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_contraseña_segura
POSTGRES_DB=tu_nombre_de_la_base_de_datos

# --- TELEGRAM ALERT CONFIGURATION ---
TELEGRAM_TOKEN=tu_token_telegram
TELEGRAM_CHAT_ID=@AirWatch_London
ALERT_CHECK_INTERVAL=1800

# --- GRAFANA ---
GRAFANA_ADMIN_USER=tu_usuario
GRAFANA_ADMIN_PASSWORD=tu_contraseña_segura
```

### 3️⃣ Generar API Keys Internas

#### Iniciar solo la API y la Base de Datos
```bash
docker-compose up -d db api-db
```

Accede a la documentación en:  
🌐 **http://127.0.0.1:8000/docs**

#### Tiempo Real

1. Haz clic en el botón verde **`POST /registrar`**
2. Registra un usuario, por ejemplo: `bot_waqi`
3. Copia el valor de **`tu_api_key`** que te devuelve
4. Abre tu archivo **`.env`** y pega la llave en:
```env
API_TOKEN_APIDB_REALTIME=pega_la_nueva_llave_aqui
```

#### Históricos

1. Haz clic nuevamente en **`POST /registrar`**
2. Registra otro usuario, por ejemplo: `bot_openaq`
3. Copia el valor de **`tu_api_key`**
4. Abre tu archivo **`.env`** y pega la llave en:
```env
API_TOKEN_APIDB_HISTORICAL=pega_la_nueva_llave_aqui
```

### 4️⃣ Levantar Todos los Servicios
```bash
docker-compose up -d
```

> ⏱️ **Primera vez**: tardará ~2 minutos en descargar imágenes y crear la base de datos. Luego esperará 30 segundos antes de que DBT empiece a transformar datos.

---

## 🔑 Acceso a los Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| 🗺️ **Mapa Plotly/Dash** | http://localhost:8050 | Acceso público |
| 📊 **Grafana** | http://localhost:3000 | Definidas en `.env` |

---

## 🎯 Capas de DBT

El proyecto usa 3 capas de transformación:

### 1. Staging: `stg__air_quality`
- Limpia y normaliza los datos crudos
- Convierte tipos de datos
- Filtra valores negativos (ruido del sensor)

### 2. Intermediate

**`int__aqi_calculations`**
- Calcula el índice AQI según EPA (EE.UU.)

**`int__historical_ref`**
- Calcula promedios y desviación estándar histórica por zona

### 3. Mart: `marts__alerts`

Genera 3 tipos de alertas:
- **Absoluta**: supera límites OMS (ej: PM2.5 > 25 µg/m³)
- **Sensible**: niveles peligrosos para grupos vulnerables
- **Relativa**: valor 2.5x mayor que la media histórica de esa zona

---

## 🔔 Tipos de Alertas

El sistema envía notificaciones a Telegram cuando:

| Tipo | Condición | Ejemplo |
|------|-----------|---------|
| 🔴 **Crítica** | Supera límites OMS | PM2.5 > 25 µg/m³ |
| 🟠 **Sensible** | Riesgo para asmáticos, niños, ancianos | PM2.5 > 15 µg/m³ |
| 🟡 **Anomalía** | Pico inusual vs histórico | 2.5x por encima del promedio |

**Frecuencia**: cada 30 minutos (configurable en `.env`)

---

## 📝 Colaboradores

Este proyecto es parte de un Máster en Big Data y Cloud.

- Aisha del Tío de Prado 
- Miguel Ángel Navarro
- Carlos Gil
- Ricardo Manuel Edreira Penas