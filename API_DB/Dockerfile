FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

# Start
CMD ["uvicorn", "api_db:app", "--host", "0.0.0.0", "--port", "8000"]

# http://127.0.0.1:8000/docs