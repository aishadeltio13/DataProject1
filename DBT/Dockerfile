FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Find profiles.yml
ENV DBT_PROFILES_DIR=/app

# Container on
CMD ["tail", "-f", "/dev/null"]