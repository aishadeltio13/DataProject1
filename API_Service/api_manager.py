import os
import sys
import time
import requests
import schedule
from datetime import datetime

# --- CONFIGURATION API WAQI ---
TOKEN_WAQI = os.getenv("WAQI_API_TOKEN") 
BOUNDS = os.getenv("LONDON_BOUNDS")
CHECK_INTERVAL = int(os.getenv("SCAN_INTERVAL"))

# --- CONFIGURATION API DB ---
API_URL = os.getenv("API_URL")
MI_API_KEY = os.getenv("API_TOKEN_APIDB_REALTIME")

MAP_URL = f"https://api.waqi.info/map/bounds/?latlng={BOUNDS}&token={TOKEN_WAQI}"
TARGET_POLLUTANTS = ["pm25", "pm10", "no2", "o3"]

def fetch_station_details(uid):
    try:
        return requests.get(f"https://api.waqi.info/feed/@{uid}/?token={TOKEN_WAQI}", timeout=10).json()
    except:
        return None

def enviar_a_la_api(payload):
    headers = {"x-api-key": MI_API_KEY}
    try:
        # Send with header (API-KEY)
        requests.post(API_URL, json=payload, headers=headers)
        return True
    except:
        return False
    

def run_ingestion_cycle():
    cycle_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n--- Recolecting data ({cycle_time}) ---", file=sys.stdout)
    
    try:
        map_res = requests.get(MAP_URL).json()
        stations = map_res.get("data", [])
    except Exception as e:
        print(f"ERROR connecting with WAQI Map: {e}")
        return

    saved_count = 0
    for station in stations:
        uid = station.get("uid")
        time.sleep(0.1) 
        
        full_res = fetch_station_details(uid)
        
        if full_res and full_res.get("status") == "ok":
            data = full_res.get("data", {})
            iaqi = data.get("iaqi", {})
            
            # Date
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sensor_time = data.get("time", {}).get("s") 

            for pollutant in TARGET_POLLUTANTS:
                if pollutant in iaqi:
                    reading_data = iaqi[pollutant]
                    val = reading_data.get("v")
                    
                    # Float 
                    try:
                        lat = float(data.get("city", {}).get("geo", [])[0])
                        lon = float(data.get("city", {}).get("geo", [])[1])
                        val = float(val)
                    except:
                        continue # No numbers, skip (remember the rules)

                    payload = {
                        "source": "realtime",
                        "station_uid": int(data.get("idx")), 
                        "station_name": str(data.get("city", {}).get("name")),
                        "lat": lat,
                        "lon": lon,
                        "sensor_date": str(sensor_time),
                        "scraped_at": now_str,
                        "parameter": pollutant,
                        "value": val,
                        "unit": "aqi"  # WAQI unit
                    }

                    if enviar_a_la_api(payload):
                        saved_count += 1
                        sys.stdout.write(".") # See how it is working (know where are problems)
                        sys.stdout.flush()

    print(f"\n--- Done. Ingested in BD: {saved_count} registries. ---")

if __name__ == "__main__":
    print(f"Starting Ingestor {API_URL}")
    
    run_ingestion_cycle()
    
    schedule.every(CHECK_INTERVAL).seconds.do(run_ingestion_cycle)
    
    while True:
        schedule.run_pending()
        time.sleep(1)