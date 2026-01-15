import time
import requests
import os
import json

# --- CONFIGURATION ---
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
BBOX = os.getenv("OPENAQ_BOUNDS") 

GATEKEEPER_URL = "http://db_security:8000/ingest"
V3_BASE_URL = "https://api.openaq.org/v3"

# Official Pollutant IDs in OpenAQ V3
# Reference: https://docs.openaq.org/resources/parameters
POLLUTANT_IDS = {
    "pm25": 2,
    "pm10": 1,
    "no2": 5,
    "o3": 3,
    "co": 4,
    "so2": 6
}

# Years to download (Expand if you need more history)
TARGET_YEARS = [2024, 2025] 

def get_headers():
    if not OPENAQ_API_KEY:
        print("ERROR: OPENAQ_API_KEY missing in .env file")
        return None
    return {"X-API-Key": OPENAQ_API_KEY}

def fetch_and_load_history():
    print(f" [HISTORICAL V3 - HYBRID MODE] Starting mass extraction...")
    
    headers = get_headers()
    if not headers or not BBOX: 
        print("ERROR: Check your configuration (.env). API Key or BBOX missing.")
        return

    # 1. LOOP BY POLLUTANT (Sweep Strategy)
    for name, p_id in POLLUTANT_IDS.items():
        print(f"\n Searching for locations measuring {name.upper()} (ID: {p_id}) in London...")
        
        # STEP A: Search Locations filtered by BBOX and Parameter ID
        # Documentation: GET /v3/locations
        locations_url = f"{V3_BASE_URL}/locations"
        params = {
            "bbox": BBOX,
            "parameters_id": p_id, 
            "limit": 1000  # Max allowed per page
        }
        
        try:
            res = requests.get(locations_url, params=params, headers=headers, timeout=20)
            if res.status_code != 200:
                print(f"API Error searching locations: {res.text}")
                continue
                
            locations = res.json().get("results", [])
            print(f"✅ Found {len(locations)} locations with {name}!")

        except Exception as e:
            print(f"Connection error: {e}")
            continue

        # STEP B: Process each Location to find its Sensor
        for i, loc in enumerate(locations):
            loc_id = loc.get("id")
            loc_name = loc.get("name")
            
            # Now we ask for SPECIFIC sensors for this location
            # Documentation: GET /v3/locations/{id}/sensors
            sensors_url = f"{V3_BASE_URL}/locations/{loc_id}/sensors"
            try:
                s_res = requests.get(sensors_url, headers=headers, timeout=10)
                sensors = s_res.json().get("results", [])
            except:
                print(f"Error getting sensors for {loc_name}, skipping...")
                continue

            # Filter in memory to keep only the sensor for the current pollutant
            target_sensor = None
            for s in sensors:
                if s.get("parameter", {}).get("id") == p_id:
                    target_sensor = s
                    break
            
            if not target_sensor: 
                continue # If for some reason it's not there, skip

            sensor_id = target_sensor.get("id")
            print(f"   📡 [{i+1}/{len(locations)}] {loc_name} -> Downloading history for Sensor {sensor_id}...")

            # STEP C: Download Historical Measurements
            # Documentation: GET /v3/sensors/{id}/measurements
            for year in TARGET_YEARS:
                meas_url = f"{V3_BASE_URL}/sensors/{sensor_id}/measurements"
                page = 1
                
                while True:
                    # Use datetime_from in ISO 8601 format
                    m_params = {
                        "datetime_from": f"{year}-01-01T00:00:00Z",
                        "datetime_to": f"{year}-12-31T23:59:59Z",
                        "limit": 1000,
                        "page": page
                    }
                    
                    try:
                        m_res = requests.get(meas_url, params=m_params, headers=headers, timeout=10)
                        
                        if m_res.status_code == 429:
                            print("API Paused 45s (Rate Limit)...")
                            time.sleep(45)
                            continue
                        if m_res.status_code != 200: break
                        
                        measurements = m_res.json().get("results", [])
                        if not measurements: break # If no data in target year, stop and skip to next.

                        # Send to DB Security 
                        for m in measurements:
                            payload = {
                                "source": "openaq_v3_history",
                                "station_uid": sensor_id, # Use Sensor ID for maximum precision
                                "station_name": loc_name,
                                # Coordinates come from Location
                                "lat": loc.get("coordinates", {}).get("latitude"),
                                "lon": loc.get("coordinates", {}).get("longitude"),
                                "date": m.get("period", {}).get("datetimeFrom", {}).get("utc"),
                                "parameter": name,
                                "value": str(m.get("value")),
                                "unit": m.get("unit"),
                                "full_json": m
                            }
                            try:
                                requests.post(GATEKEEPER_URL, json=payload, timeout=0.2)
                            except:
                                pass # Fire and forget 
                        
                        page += 1
                        time.sleep(0.1) 

                    except Exception as e:
                        print(f"Error reading measurements: {e}")
                        break
    
    print("\n HISTORICAL SWEEP COMPLETE.")

if __name__ == "__main__":
    print("⏳ Waiting 10s for Database to start...")
    time.sleep(10)
    fetch_and_load_history()