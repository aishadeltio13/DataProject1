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
START_YEAR = 2025

# --- DATA WANTED (REDUCED) ---
TARGET_DAYS = [2, 7, 12, 17, 22, 27]       # 6 days per mont
TARGET_HOURS = [0, 4, 8, 12, 16, 20]       # Every 4 hours


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
    ahora = datetime.now()
    
    for pol_name, pol_id in POLLUTANTS.items():
        print(f"\nSearching stations for {pol_name}...", flush=True)
        
        # 1. Search for stations in the area
        locs_resp = get_json(f"{BASE_URL}/locations", {"bbox": BBOX, "parameters_id": pol_id, "limit": 1000})
        if not locs_resp: continue
        
        locations = locs_resp.get("results", [])
        print(f"Founded {len(locations)} stations.", flush=True)

        for loc in locations:
            # 2. Search for sensors of each station
            sensors_resp = get_json(f"{BASE_URL}/locations/{loc['id']}/sensors")
            if not sensors_resp: continue
            
            # Only keep the sensor that measures the pollutant wanted
            target = next((s for s in sensors_resp['results'] if s['parameter']['id'] == pol_id), None)
            if not target: continue
            
            unit = target['parameter'].get('units')

            # 3. Download (REDUCED STRATEGY)
            print(f"  > Processing {loc['name']} (ID: {loc['id']})...", flush=True)
            page = 1
            total_inserted = 0
            
            # 2025-actualyear
            for year in range(START_YEAR, ahora.year + 1):
                 # 12 month
                for month in range(1, 13):
                    # Days we want
                    for day in TARGET_DAYS:
        
                        try:
                            fecha_peticion = datetime(year, month, day)
                            if fecha_peticion > ahora:
                                continue 
                        except ValueError:
                            continue 
                        
                        # Exact day
                        date_from = f"{year}-{month:02d}-{day:02d}T00:00:00Z"
                        date_to = f"{year}-{month:02d}-{day:02d}T23:59:59Z"

                        # Data from that day
                        m_resp = get_json(f"{BASE_URL}/sensors/{target['id']}/measurements", {
                            "datetime_from": date_from,
                            "datetime_to": date_to,
                            "limit": 100,
                            "page": 1
                        })
                        
                        if not m_resp or not m_resp.get("results"): continue

                        for m in m_resp["results"]:
                            
                            raw_date = m['period']['datetimeFrom']['utc']
                            dt_obj = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%SZ" ) # Object to see the time
                            
                            # Save if the time is in TARGET_HOURS
                            if dt_obj.hour in TARGET_HOURS:
                                
                                # --- FORMAT DATE (RULES) ---
                                # OpenAQ da: "2025-01-01T10:00:00Z"
                                # API DB: "2025-01-01 10:00:00"
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
                                    "unit": str(unit) 
                                }

                                if enviar_a_la_api(payload):
                                    total_inserted += 1
                        
                        # Princess pause
                        time.sleep(0.05)

                print(f"    Done. {total_inserted} records inserted (Reduced Strategy).", flush=True)

        print("FINISH PROCESS.", flush=True)

if __name__ == "__main__":
    # Pause to make sure everything has started
    time.sleep(10)
    main()