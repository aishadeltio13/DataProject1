FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# We expose port 8000 so API_Service and HistoricalData can call 
CMD ["uvicorn", "DbApi:app", "--host", "0.0.0.0", "--port", "8000"]