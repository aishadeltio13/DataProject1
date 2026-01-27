#!/bin/bash
set -e

echo "DBT Orchestration - London Air Quality"

echo "Waiting for database and initial data (30 seconds)..."
sleep 30

# First immediate execution
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Initial DBT run..."
dbt run --select staging
dbt run --select intermediate
dbt run --select mart
dbt test || echo "Some tests failed"
echo "Initial DBT run completed!"

# Infinite loop running every 10 minutes
while true; do
    echo ""
    echo "Waiting 30 minutes for next run..."
    sleep 180
    
    echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Starting DBT run..."
    
    dbt run --select staging
    dbt run --select intermediate  
    dbt run --select mart
    dbt test || echo "Some tests failed"
    
    echo "✅ [$(date '+%Y-%m-%d %H:%M:%S')] DBT run completed!"
done