import os # Read the variables from the .env file.
import requests # Browse the internet.
import pandas as pd # Data manipulation in table format
from datetime import datetime

# --- CONFIGURATION ---
TOKEN = os.getenv("API_TOKEN")
url = f"https://api.waqi.info/feed/London/?token={TOKEN}"
OUTPUT_PATH = "/app/output/london_history.csv"

# URL DB_API (SECURITY)
GATEKEEPER_URL = "http://db_api:8000/ingest"  # Docker service name and port to listen on, and the specific endpoint where it handles requests.

def get_historical_data():
    """
    Extracts data from JSON
    Returns a CSV with the raw data
    """
    print(f"Searching historic data for London")
    try:
        response = requests.get(url)
        data = response.json()

        if data["status"] == "ok":
            history_points = data["data"]["forecast"]["daily"]

            records = []
            for poll_type, readings in history_points.items():
                for r in readings:
                    records.append({
                        "date": r['day'],
                        "parameter": poll_type,
                        "value": r['avg'],
                    })
            
            # Save data as csv
            df = pd.DataFrame(records)
            df.to_csv(OUTPUT_PATH, index=False, mode="a", header=not os.path.exists(OUTPUT_PATH))
            print(f"CSV updated in: {OUTPUT_PATH}")
            
            # Send data to DB_API
            requests.post(GATEKEEPER_URL, json=data)
            print(f"Historical data sent to DB_API")
    except Exception as e:
        print(f"Error with historical data: {e}")

if __name__ == "__main__":
    get_historical_data()
