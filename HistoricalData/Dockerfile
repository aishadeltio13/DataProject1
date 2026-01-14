FROM python:3.11-slim
WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Data will be saved in this directory
RUN mkdir -p /app/output

CMD ["python", "historical_loader.py"]