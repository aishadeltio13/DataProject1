FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Search for DBT files and information.
ENV DBT_PROFILES_DIR=/app

# Keep docker alive.
CMD ["tail", "-f", "/dev/null"]