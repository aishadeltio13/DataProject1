import asyncio
import aiohttp
import os
import json
import time
import random

# --- CONFIGURATION ---
API_KEY = os.getenv("OPENAQ_API_KEY")
BBOX = os.getenv("OPENAQ_BOUNDS")
GATEKEEPER_URL = "http://db_security:8000/ingest"
BASE_URL = "https://api.openaq.org/v3"

# WORKERS: 4 concurrent processes (Safe for API limits)
MAX_CONCURRENT_REQUESTS = 4

POLLUTANTS = {"pm25": 2, "pm10": 1, "no2": 5, "o3": 3}
YEARS = [2025]

async def send_to_db(session, payload):
    try:
        await session.post(GATEKEEPER_URL, json=payload)
    except:
        pass

async def process_location(session, loc, pollutant_name, pollutant_id, semaphore):
    loc_id = loc.get("id")
    loc_name = loc.get("name")
    
    # 1. GET SENSOR ID (Now part of the parallel worker)
    # We add jitter here too to avoid hitting /sensors endpoint all at once
    await asyncio.sleep(random.uniform(0.1, 1.5))
    
    sensor_id = None
    async with semaphore:
        try:
            url = f"{BASE_URL}/locations/{loc_id}/sensors"
            headers = {"X-API-Key": API_KEY}
            async with session.get(url, headers=headers) as r:
                if r.status == 200:
                    data = await r.json()
                    sensors = data.get("results", [])
                    # Find the specific sensor for this pollutant
                    target = next((s for s in sensors if s.get("parameter", {}).get("id") == pollutant_id), None)
                    if target:
                        sensor_id = target.get("id")
        except:
            return

    if not sensor_id: return

    # 2. DOWNLOAD HISTORY
    print(f"   ⬇️ Starting download for {loc_name} ({pollutant_name})...")
    
    for year in YEARS:
        page = 1
        while True:
            params = {
                "datetime_from": f"{year}-01-01T00:00:00Z",
                "datetime_to": f"{year}-12-31T23:59:59Z",
                "limit": 1000,
                "page": page
            }
            
            async with semaphore:
                try:
                    async with session.get(f"{BASE_URL}/sensors/{sensor_id}/measurements", params=params, headers=headers) as r:
                        if r.status == 429:
                            print(f"⏳ Rate limit at {loc_name}. Cooling down 60s...")
                            await asyncio.sleep(65)
                            continue
                        if r.status != 200: break
                        
                        data = await r.json()
                        measurements = data.get("results", [])
                        if not measurements: break

                        # Send batch to DB
                        tasks = []
                        for m in measurements:
                            payload = {
                                "source": "openaq_v3_history",
                                "station_uid": sensor_id,
                                "station_name": loc_name,
                                "lat": loc.get("coordinates", {}).get("latitude"),
                                "lon": loc.get("coordinates", {}).get("longitude"),
                                "date": m.get("period", {}).get("datetimeFrom", {}).get("utc"),
                                "parameter": pollutant_name,
                                "value": str(m.get("value")),
                                "unit": m.get("unit"),
                                "full_json": m
                            }
                            tasks.append(send_to_db(session, payload))
                        
                        await asyncio.gather(*tasks)
                        
                        # Only print every 5 pages to reduce log noise, or first page
                        if page % 5 == 0 or page == 1:
                            print(f"✅ {loc_name}: Saved {len(measurements)} rows ({year} Pg {page})")
                        
                        page += 1
                        await asyncio.sleep(0.5)

                except Exception as e:
                    print(f"❌ Error {loc_name}: {e}")
                    break

async def main():
    print("🚀 STARTING INSTANT WORKERS...")
    await asyncio.sleep(5) 
    
    if not API_KEY or not BBOX: return
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        for name, p_id in POLLUTANTS.items():
            print(f"\n🔎 Scanning for {name}...")
            
            # Get List of Locations
            async with session.get(f"{BASE_URL}/locations", params={"bbox": BBOX, "parameters_id": p_id, "limit": 1000}, headers={"X-API-Key": API_KEY}) as r:
                if r.status != 200: 
                    print("Error getting locations")
                    continue
                data = await r.json()
                locations = data.get("results", [])

            print(f"📋 Found {len(locations)} locations. Processing immediately...")

            # Create tasks for EVERYTHING at once
            tasks = []
            for loc in locations:
                tasks.append(process_location(session, loc, name, p_id, semaphore))
            
            # Run everything
            await asyncio.gather(*tasks)

    print("🏁 EXTRACTION COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())