import os
import json
import time
import threading
import logging
import sqlite3
from flask import Flask, jsonify
from confluent_kafka import Consumer, Producer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
CONTROL_TOPIC = os.getenv('CONTROL_TOPIC', 'heating-system-control')
SET_TEMPERATURE_TOPIC = os.getenv('SET_TEMPERATURE_TOPIC', 'heating-system-set-temperature')
DEVICE_STATUS_TOPIC = os.getenv('DEVICE_STATUS_TOPIC', 'heating-system-status-updates')

app = Flask(__name__)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'device_management_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([CONTROL_TOPIC, SET_TEMPERATURE_TOPIC])


DATABASE = 'devices.db'


def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS devices (
                        id TEXT PRIMARY KEY,
                        status TEXT,
                        temperature REAL,
                        last_updated TIMESTAMP
                      )''')
    conn.commit()
    conn.close()


init_db()


def get_device_from_db(device_id):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, status, temperature, last_updated FROM devices WHERE id = ?", (device_id,))
    device = cursor.fetchone()
    conn.close()
    return device


def update_device_in_db(device_id, status=None, temperature=None):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    if status:
        cursor.execute("INSERT OR REPLACE INTO devices (id, status, temperature, last_updated) VALUES (?, ?, COALESCE((SELECT temperature FROM devices WHERE id = ?), ?), ?)",
                       (device_id, status, device_id, temperature, datetime.now()))
    elif temperature is not None:
        cursor.execute("INSERT OR REPLACE INTO devices (id, status, temperature, last_updated) VALUES (?, COALESCE((SELECT status FROM devices WHERE id = ?), 'OFF'), ?, ?)",
                       (device_id, device_id, temperature, datetime.now()))
    conn.commit()
    conn.close()


def send_status_update(device_id, status, temperature=None):
    message = {
        "event": "status_update",
        "device_id": device_id,
        "status": status,
        "temperature": temperature
    }
    producer.produce(DEVICE_STATUS_TOPIC, key=device_id, value=json.dumps(message))
    producer.flush()


def consume_messages():
    time.sleep(5)  # Ждем, пока Kafka готова
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        message = json.loads(msg.value().decode('utf-8'))
        device_id = msg.key().decode('utf-8')
        topic = msg.topic()

        if topic == CONTROL_TOPIC:
            command = message.get('command')
            logger.info(f"Executing command {command} for device {device_id}")
            if command == "TURN_ON":
                update_device_in_db(device_id, status="ON")
                send_status_update(device_id, "ON")
            elif command == "TURN_OFF":
                update_device_in_db(device_id, status="OFF")
                send_status_update(device_id, "OFF")
        elif topic == SET_TEMPERATURE_TOPIC:
            temperature = message.get('temperature')
            logger.info(f"Setting temperature {temperature} for device {device_id}")
            update_device_in_db(device_id, temperature=temperature)
            send_status_update(device_id, "TEMPERATURE_SET", temperature)


consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()


@app.route('/devices/<device_id>/status', methods=['GET'])
def get_device_status(device_id):
    device = get_device_from_db(device_id)
    if device:
        return jsonify({"device_id": device[0], "status": device[1], "temperature": device[2], "last_updated": device[3]})
    else:
        return jsonify({"device_id": device_id, "status": "unknown"}), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
