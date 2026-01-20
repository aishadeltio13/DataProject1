import time
import requests
import os
import sys
from datetime import datetime

# --- CONFIGURATION OPENAQ ---
API_KEY = os.getenv("OPENAQ_API_KEY")
BBOX = os.getenv("OPENAQ_BOUNDS")
BASE_URL = "https://api.openaq.org/v3"

# --- CONFIGURATION TU API ---
API_URL = os.getenv("API_URL")
MI_API_KEY = os.getenv("API_TOKEN_APIDB_HISTORICAL")

POLLUTANTS = {"pm25": 2, "pm10": 1, "no2": 5, "o3": 3}
YEAR = 2025

session = requests.Session()
session.headers.update({"X-API-Key": API_KEY})

def get_json(url, params=None):
    try:
        r = session.get(url, params=params, timeout=30)
        # Respect limits of the API (avoid being blocked)
        if r.status_code == 429:
            print("Stop 60 seconds...", flush=True)
            time.sleep(60)
            return get_json(url, params)
        return r.json()
    except:
        return None

def enviar_a_la_api(payload):
    headers = {"x-api-key": MI_API_KEY}
    try:
        requests.post(API_URL, json=payload, headers=headers)
        return True
    except:
        return False
    

def main():
    print(f"Starting historic download: {API_URL}", flush=True)
    time.sleep(5) # Wait until DB is ready
    
    for pol_name, pol_id in POLLUTANTS.items():
        print(f"\nSearching stations for {pol_name}...", flush=True)
        
        # 1. Search for stations in the area
        locs_resp = get_json(f"{BASE_URL}/locations", {"bbox": BBOX, "parameters_id": pol_id, "limit": 1000})
        if not locs_resp: continue
        
        locations = locs_resp.get("results", [])
        print(f"Encontradas {len(locations)} estaciones.", flush=True)

        for loc in locations:
            # 2. Search for sensors of each station
            sensors_resp = get_json(f"{BASE_URL}/locations/{loc['id']}/sensors")
            if not sensors_resp: continue
            
            # Only keep the sensor that measures the pollutant wanted
            target = next((s for s in sensors_resp['results'] if s['parameter']['id'] == pol_id), None)
            if not target: continue
            
            unit = target['parameter'].get('units')

            # 3. Download
            print(f"  > Processing {loc['name']} (ID: {loc['id']})...", flush=True)
            page = 1
            total_inserted = 0
            
            while True:
                m_resp = get_json(f"{BASE_URL}/sensors/{target['id']}/measurements", {
                    "datetime_from": f"{YEAR}-01-01T00:00:00Z",
                    "datetime_to": f"{YEAR}-12-31T23:59:59Z",
                    "limit": 1000,
                    "page": page
                })
                
                if not m_resp or not m_resp.get("results"): break

                for m in m_resp["results"]:
                    
                    # --- FORMAT DATE (RULES) ---
                    # OpenAQ da: "2025-01-01T10:00:00Z"
                    # API DB: "2025-01-01 10:00:00"
                    raw_date = m['period']['datetimeFrom']['utc']
                    clean_date = raw_date.replace("T", " ").replace("Z", "")

                    payload = {
                        "source": "historical_data",
                        "station_uid": int(target['id']),
                        "station_name": str(loc['name']),
                        "lat": float(loc['coordinates']['latitude']),
                        "lon": float(loc['coordinates']['longitude']),
                        "sensor_date": clean_date,  # Clean date
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "parameter": pol_name,
                        "value": float(m['value']),
                        "unit": str(unit) # API DB will check ('aqi' o 'µg/m³')
                    }

                    if enviar_a_la_api(payload):
                        total_inserted += 1

                print(f"    Page {page}: {len(m_resp['results'])} read / {total_inserted} inserted.", flush=True)
                page += 1
                time.sleep(0.2) # Respeto a la API de OpenAQ

    print("FINISH PROCESS.", flush=True)

if __name__ == "__main__":
    # Pause to make sure everything has started
    time.sleep(10)
    main()