#!/usr/bin/env python3
"""
processor.py
Data preprocessing operator:
 - Subscribe to EMQX MQTT topic (pm25/data)
 - Print each reading to console (docker logs)
 - Filter outliers (>50) and print them
 - Average PM2.5 by UTC day (24-hour buckets)
 - Publish daily averages to RabbitMQ (AMQP) queue 'pm25_processed'
 - Avoid duplicate publishes; publish completed days only
"""

import os
import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import pika

# Configuration via environment variables (Docker Compose sets defaults)
EMQX_HOST = os.getenv("EMQX_HOST", "localhost")
EMQX_PORT = int(os.getenv("EMQX_PORT", 1883))
EMQX_TOPIC = os.getenv("EMQX_TOPIC", "pm25/data")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "pm25_processed")

# Where processed daily averages are stored before publishing
daily_values = defaultdict(list)
sent_dates = set()
lock = threading.Lock()


def parse_timestamp(ts_field):
    if ts_field is None:
        return None
    if isinstance(ts_field, (int, float)):
        if ts_field > 1e12:
            return ts_field / 1000.0
        return float(ts_field)
    if isinstance(ts_field, str):
        if ts_field.isdigit():
            n = int(ts_field)
            if n > 1e12:
                return n / 1000.0
            return float(n)
        try:
            dt = datetime.fromisoformat(ts_field.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts_field, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                continue
    return None


def send_to_rabbitmq(payload):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        body = json.dumps(payload)
        channel.basic_publish(exchange="", routing_key=RABBITMQ_QUEUE, body=body)
        connection.close()
        print(f"📤 [AMQP] Published to {RABBITMQ_QUEUE}: {body}")
    except Exception as e:
        print(f"⚠️ [AMQP] Failed to publish: {e}")


def process_message_json(data):
    ts = None
    val = None
    if isinstance(data, dict):
        ts_candidates = ["timestamp", "Timestamp", "time", "ts", "date"]
        val_candidates = ["value", "Value", "pm25", "pm2_5", "pm2.5"]
        for k in ts_candidates:
            if k in data:
                ts = data[k]
                break
        for k in val_candidates:
            if k in data:
                val = data[k]
                break
    ts_seconds = parse_timestamp(ts)
    if ts_seconds is None:
        ts_seconds = time.time()
    try:
        val_float = float(val)
    except Exception:
        return None, None
    return ts_seconds, val_float


def on_mqtt_message(client, userdata, msg):
    try:
        payload = msg.payload.decode(errors="ignore")
        print(f"📥 [MQTT] Topic: {msg.topic} Payload: {payload}")
        data = None
        try:
            data = json.loads(payload)
        except Exception:
            print("⚠️ [MQTT] Received non-JSON payload, ignoring.")
            return

        ts_seconds, value = process_message_json(data)
        if ts_seconds is None or value is None:
            print("⚠️ [PROCESS] Could not extract timestamp/value, skipping.")
            return

        dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        date_str = dt.date().isoformat()

        if value > 50:
            print(f"🚫 [OUTLIER] {value} at {dt.isoformat()} (topic={msg.topic})")
            return

        # ✅ Add value for daily average calculation
        with lock:
            daily_values[date_str].append(value)

        # ✅ Immediately publish every incoming message to RabbitMQ
        payload_to_publish = {
            "timestamp": dt.isoformat(),
            "topic": msg.topic,
            "pm25": value
        }
        send_to_rabbitmq(payload_to_publish)

        print(f"✅ [ACCEPTED] {value} at {dt.isoformat()} (published immediately)")

    except Exception as e:
        print(f"⚠️ [ERR] Exception in on_mqtt_message: {e}")

def compute_and_publish_completed_days():
    while True:
        try:
            to_publish = []
            with lock:
                for date_str, values in daily_values.items():
                    if not values:
                        continue
                    if date_str not in sent_dates:
                        avg = round(sum(values) / len(values), 2)
                        to_publish.append((date_str, avg))
            for date_str, avg in to_publish:
                payload = {"date": date_str, "avg_pm25": avg}
                print(f"🔁 [DAILY AVG] {payload} — publishing to RabbitMQ")
                send_to_rabbitmq(payload)
                with lock:
                    sent_dates.add(date_str)
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ [ERR] in compute thread: {e}")
            time.sleep(5)


def main():
    t = threading.Thread(target=compute_and_publish_completed_days, daemon=True)
    t.start()

    client = mqtt.Client()
    client.on_message = on_mqtt_message

    while True:
        try:
            print(f"🔌 Connecting to EMQX at {EMQX_HOST}:{EMQX_PORT} ...")
            client.connect(EMQX_HOST, EMQX_PORT, keepalive=60)
            client.subscribe(EMQX_TOPIC)
            print(f"📡 Subscribed to topic: {EMQX_TOPIC}")
            break
        except Exception as e:
            print(f"⚠️ [MQTT] Connection failed: {e}. Retrying in 5s.")
            time.sleep(5)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("⛔ KeyboardInterrupt received - publishing remaining days and exiting...")
        now_utc = datetime.now(timezone.utc)
        today_str = now_utc.date().isoformat()
        with lock:
            for date_str, values in list(daily_values.items()):
                if not values:
                    continue
                if date_str < today_str and date_str not in sent_dates:
                    avg = round(sum(values) / len(values), 2)
                    payload = {"date": date_str, "avg_pm25": avg}
                    send_to_rabbitmq(payload)
                    sent_dates.add(date_str)
        print("✔️ Done. Exiting.")
    except Exception as e:
        print(f"⚠️ [MAIN] Exception: {e}")


if __name__ == "__main__":
    main()
