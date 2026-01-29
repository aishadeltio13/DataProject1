import os
import sys
import time
import requests
import psycopg2
from datetime import datetime, timedelta
import schedule

# =============================================================================
# CONFIGURATION
# =============================================================================

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
CHECK_INTERVAL_SECONDS = int(os.getenv("ALERT_CHECK_INTERVAL", "1800"))

WHO_LIMITS = {
    'pm25': 25,
    'pm10': 50,
    'no2': 200,
    'o3': 120
}

last_alerts_sent = {}

# =============================================================================
# FUNCTIONS
# =============================================================================

def wait_for_database(max_retries=30, retry_interval=2):
    print(f"Waiting for database to be ready at {DB_HOST}:{DB_PORT}...")

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                connect_timeout=5
            )
            conn.close()
            print(f"✓ Database connection successful (attempt {attempt}/{max_retries})")
            return True
        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                print(f"✗ Database not ready yet (attempt {attempt}/{max_retries}). Retrying in {retry_interval}s...")
                time.sleep(retry_interval)
            else:
                print(f"✗ Failed to connect to database after {max_retries} attempts")
                print(f"Error: {e}")
                return False

    return False

def get_active_alerts():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        query = """
            SELECT
                station_name,
                area_code,
                sensor_date,
                parameter,
                measurement_value,
                unit,
                realtime_aqi_value,
                aqi_category,
                absolute_alert,
                sensitive_alert,
                relative_alert
            FROM marts__alerts
            WHERE sensor_date > NOW() - INTERVAL '30 minutes'
            AND (absolute_alert = TRUE OR sensitive_alert = TRUE OR relative_alert = TRUE)
            ORDER BY sensor_date DESC, realtime_aqi_value DESC
            LIMIT 100
        """

        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        alerts = []
        for row in rows:
            alerts.append({
                "station_name": row[0],
                "area_code": row[1],
                "sensor_date": row[2],
                "parameter": row[3],
                "measurement_value": row[4],
                "unit": row[5],
                "aqi_value": row[6],
                "aqi_category": row[7],
                "absolute_alert": row[8],
                "sensitive_alert": row[9],
                "relative_alert": row[10]
            })

        print(f"[DB] {len(alerts)} active alerts found")
        return alerts

    except Exception as err:
        print(f"Database Error: {err}")
        return []

def format_alert_message(alerts):
    if not alerts:
        return None

    critical_alerts = []
    sensitive_alerts = []
    relative_alerts = []

    for alert in alerts:
        alert_key = f"{alert['station_name']}_{alert['parameter']}_{alert['sensor_date']}"

        if alert_key in last_alerts_sent:
            continue

        last_alerts_sent[alert_key] = datetime.now()

        if alert['absolute_alert']:
            critical_alerts.append(alert)
        elif alert['sensitive_alert']:
            sensitive_alerts.append(alert)
        elif alert['relative_alert']:
            relative_alerts.append(alert)

    if not (critical_alerts or sensitive_alerts or relative_alerts):
        return None

    message = "🚨 **AIR QUALITY ALERT - LONDON** 🚨\n"
    message += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    if critical_alerts:
        message += "🔴 **CRITICAL ALERTS (WHO Limits Exceeded)**\n"
        for alert in critical_alerts[:5]:
            pollutant = alert['parameter'].upper()
            value = alert['measurement_value']
            limit = WHO_LIMITS.get(alert['parameter'], 'N/A')
            station = alert['station_name']
            aqi = alert['aqi_value']

            message += f"  ⚠️ {pollutant}: {value:.1f} µg/m³ (Limit: {limit})\n"
            message += f"     Station: {station}\n"
            message += f"     AQI: {aqi:.0f} ({alert['aqi_category']})\n\n"

    if sensitive_alerts:
        message += "🟠 **SENSITIVE GROUP ALERTS**\n"
        for alert in sensitive_alerts[:3]:
            pollutant = alert['parameter'].upper()
            value = alert['measurement_value']
            station = alert['station_name']

            message += f"  ⚠️ {pollutant}: {value:.1f} µg/m³\n"
            message += f"     Station: {station}\n\n"

    if relative_alerts:
        message += "🟡 **UNUSUAL SPIKE ALERTS**\n"
        for alert in relative_alerts[:3]:
            pollutant = alert['parameter'].upper()
            value = alert['measurement_value']
            station = alert['station_name']

            message += f"  📈 {pollutant}: {value:.1f} µg/m³ (2.5x above average)\n"
            message += f"     Station: {station}\n\n"

    # TODO: Replace localhost with server IP/domain in production
    message += "\n🔗 View real-time map: http://localhost:8050"

    return message

def send_telegram_notification(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Warning: Telegram credentials not configured. Skipping notification.")
        print(f"Message would be:\n{message}")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }

    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            print("✓ Telegram notification sent successfully")
            return True
        else:
            print(f"✗ Error sending Telegram message: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False

def check_and_notify_alerts():
    print(f"\n{'='*60}")
    print(f"Checking air quality alerts - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    global last_alerts_sent
    cutoff_time = datetime.now() - timedelta(hours=2)
    last_alerts_sent = {k: v for k, v in last_alerts_sent.items() if v > cutoff_time}

    alerts = get_active_alerts()

    if not alerts:
        print("✓ No active alerts. Air quality within acceptable limits.")
        return

    message = format_alert_message(alerts)

    if message:
        send_telegram_notification(message)
    else:
        print("✓ Alerts already notified recently. Skipping duplicate notifications.")

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("="*60)
    print("AIR QUALITY ALERT NOTIFIER - LONDON")
    print("="*60)
    print()
    print("CONFIGURATION:")
    print(f"  - Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  - Telegram: {'Configured ✓' if TELEGRAM_TOKEN else 'NOT configured ✗'}")
    print(f"  - Check interval: {CHECK_INTERVAL_SECONDS} seconds ({CHECK_INTERVAL_SECONDS/60:.0f} min)")
    print()
    print("WHO LIMITS (µg/m³):")
    for pollutant, limit in WHO_LIMITS.items():
        print(f"  - {pollutant.upper()}: {limit}")
    print()
    print("Press Ctrl+C to stop")
    print("="*60)
    print()

    if not wait_for_database():
        print("✗ Exiting: Could not connect to database")
        sys.exit(1)

    print("Waiting 10 seconds for DBT to initialize tables...")
    time.sleep(10)

    check_and_notify_alerts()

    schedule.every(CHECK_INTERVAL_SECONDS).seconds.do(check_and_notify_alerts)

    while True:
        schedule.run_pending()
        time.sleep(1)
