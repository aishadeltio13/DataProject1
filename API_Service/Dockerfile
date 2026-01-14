FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
COPY api_manager.py .

RUN pip install -r requirements.txt

CMD ["python", "-u", "api_manager.py"]