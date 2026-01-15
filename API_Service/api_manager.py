import os # Read the variables from the .env file.
import sys # Print so they appear in the Docker console.
import time # Control time (sleep).
import requests # Browse the internet.
import schedule # Task
from datetime import datetime # Get the exact time and to include it in the logs.

# --- CONFIGURATION ---
TOKEN = os.getenv("API_TOKEN")
BOUNDS = os.getenv("LONDON_BOUNDS")
CHECK_INTERVAL = int(os.getenv("SCAN_INTERVAL")) # Integer number (int) to be used in the timer.


# URL DB_SECURITY
GATEKEEPER_URL = "http://db_security:8000/ingest"  # Docker service name and port to listen on, and the specific endpoint where it handles requests.
MAP_URL = f"https://api.waqi.info/map/bounds/?latlng={BOUNDS}&token={TOKEN}"
TARGET_POLLUTANTS = ["pm25", "pm10", "no2", "o3", "so2", "co"] # Pollutants to extract.

# It receives the unique ID (uid) of a station.
def fetch_station_details(uid):
    try:
        return requests.get(f"https://api.waqi.info/feed/@{uid}/?token={TOKEN}", timeout=10).json()
    except:
        return None

def run_ingestion_cycle():
    print(f"\n--- Collecting data to send to the API: {datetime.now()} ---", file=sys.stdout)
    
    try:
        map_res = requests.get(MAP_URL).json()
        stations = map_res.get("data", [])
    except Exception as e:
        print(f"ERROR: {e}")
        return

    sent_count = 0
    for station in stations:
        uid = station.get("uid")
        time.sleep(0.1)
        
        full_res = fetch_station_details(uid) # Fetch all data of that station.
        
        if full_res and full_res.get("status") == "ok":
            data = full_res.get("data", {})
            iaqi = data.get("iaqi", {}) # This is where individual values live

            for pollutant in TARGET_POLLUTANTS: # Extract only target pollutants
                
                # Check if this station measures this pollutant
                if pollutant in iaqi:
                    reading_data = iaqi[pollutant]
                    val = reading_data.get("v")
                    
                    # STANDARDIZED PAYLOAD 
                    payload = {
                        "source": "waqi_realtime",
                        "station_uid": data.get("idx"), 
                        "station_name": data.get("city", {}).get("name"),
                        
                        # Geo-Coordinates 
                        "lat": data.get("city", {}).get("geo", [])[0],
                        "lon": data.get("city", {}).get("geo", [])[1],
                        
                        "date": data.get("time", {}).get("s"), # Reading Time
                        
                        # The Data
                        "parameter": pollutant,
                        "value": str(val),
                        "unit": "aqi", 
                        
                        # We keep a small snippet of the original just in case
                        "full_json": reading_data 
                    }

                    # 4. Send to DbSecurity
                    try:
                        res = requests.post(GATEKEEPER_URL, json=payload, timeout=2)
                        if res.status_code == 200:
                            sent_count += 1
                    except Exception as e:
                        print(f" Connection error sending {pollutant} for UID {uid}")

    print(f"---  Cycle End. Sent {sent_count} measurement packets to DB. ---", file=sys.stdout)

if __name__ == "__main__":
    time.sleep(10) # wait until everything is ready-
    run_ingestion_cycle()
    schedule.every(CHECK_INTERVAL).seconds.do(run_ingestion_cycle)
    while True:
        schedule.run_pending()
        time.sleep(1)