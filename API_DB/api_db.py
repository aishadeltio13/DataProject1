import secrets
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Header
from sqlmodel import SQLModel, Field, Session, create_engine, select
import os
from datetime import datetime            
from pydantic import field_validator   
from sqlalchemy import UniqueConstraint  

# --- 1. CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# --- 2. TABLES ---
class RegistroAire(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str
    station_uid: int
    station_name: str
    lat: float
    lon: float
    sensor_date: str
    scraped_at: str
    parameter: str
    value: float
    unit: str
    
    # Make sure in dowloads there ir no double data 
    __table_args__ = (
        UniqueConstraint("station_uid", "sensor_date", "parameter", name="registro_unico"),
    )
    
    # 1. Validate that latitude is within London (51.28 to 51.69)
    @field_validator('lat')
    @classmethod
    def validar_latitud_londres(cls, v):
        if not (51.28 <= v <= 51.69):
            raise ValueError(f"Latitude {v} outside London")
        return v

    # 2. Validate that longitude is within London (-0.51 to 0.33)
    @field_validator('lon')
    @classmethod
    def validar_longitud_londres(cls, v):
        if not (-0.51 <= v <= 0.33):
            raise ValueError(f"Longitude {v} outside London")
        return v

    # 3. Validate Units (only 'aqi' or 'µg/m³')
    @field_validator('unit')
    @classmethod
    def validar_unidades(cls, v):
        unidades_permitidas = ["aqi", "µg/m³"]
        if v not in unidades_permitidas:
            raise ValueError(f"Incorrect unit. Only allowed: {unidades_permitidas}")
        return v

    # 4. Validate Date Format (YYYY-MM-DD HH:MM:SS)
    @field_validator('sensor_date', 'scraped_at')
    @classmethod
    def validar_formato_fecha(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise ValueError("Invalid date format. Must be 'YYYY-MM-DD HH:MM:SS'")
        return v


class Usuario(SQLModel, table=True):
    username: str = Field(primary_key=True)
    api_key: str = Field(index=True, unique=True)

SQLModel.metadata.create_all(engine)

# --- 3. VALIDATION ---
def get_session():
    with Session(engine) as session:
        yield session

def validar_api_key(x_api_key: str = Header(...), session: Session = Depends(get_session)):
    usuario = session.exec(select(Usuario).where(Usuario.api_key == x_api_key)).first()
    if not usuario:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return usuario

app = FastAPI()

# --- 4. ROUTES ---

@app.post("/registrar")
def registrar_usuario(username: str, session: Session = Depends(get_session)):
    if session.exec(select(Usuario).where(Usuario.username == username)).first():
        raise HTTPException(400, "User already exists")
    
    nueva_key = secrets.token_urlsafe(32)
    
    nuevo_usuario = Usuario(username=username, api_key=nueva_key)
    session.add(nuevo_usuario)
    session.commit()
    
    return {"mensaje": "User created", "tu_api_key": nueva_key}

@app.post("/insertar-datos")
def insertar(
    dato: RegistroAire,
    session: Session = Depends(get_session),
    usuario: Usuario = Depends(validar_api_key)
):
    session.add(dato)
    session.commit()
    return {"estado": "OK", "usuario": usuario.username}