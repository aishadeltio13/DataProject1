import os
import json # Convert the Python dictionary to text (database can understand).
import psycopg2 # Driver: Connect and send commands to PostgreSQL.
from fastapi import FastAPI, HTTPException, Request # FastAPI: web server framework; HTTPException: for standard errors (e.g., 404, 500); Request: allows receiving the raw client request, enabling acceptance of any JSON.

# --- INICIALIZACIÓN ---
app = FastAPI(title="London Guard") # The app variable is what the uvicorn DbApi:app command looks for the Dockerfile.

# --- CONFIG DB ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        ) # it tries to access PostgreSQL using the credentials, and if successful, returns the "connection".
    except Exception as e:
        print(f"Error DB: {e}")
        return None

# --- CREATE TABLE ---
@app.on_event("startup")
def startup_db_check():
    conn = get_db_connection() # Connection.
    if conn:
        try:
            cur = conn.cursor() # Cursor (speak SQL).
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS london_raw_data (
                    id SERIAL PRIMARY KEY,
                    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    raw_data JSONB
                );
            """) # Allows storing entire JSONs and performing searches within them.
            conn.commit() # Save
            conn.close() # Close
            print("Table ready.")
        except Exception as e:
            print(f"Error table: {e}")

# --- THE ENDPOINT THAT RECEIVES INFORMATION ---
@app.post("/ingest")
async def receive_data(request: Request): # Allows us to read the message body regardless of its format.
    try:
        # 1. Read JSON and transform to dicctionary. 
        data = await request.json()
        
        # 2. Conect and save.
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Save in the column 'raw_data'
        query = "INSERT INTO london_raw_data (raw_data) VALUES (%s)"
        
        # json.dumps converts dicctionary to text.
        cur.execute(query, (json.dumps(data),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {"status": "saved"}
        
    except Exception as e:
        print(f"Error guardando: {e}")
        raise HTTPException(status_code=500, detail=str(e))