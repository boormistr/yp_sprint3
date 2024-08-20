import os
import json
import time
import threading
import logging
import sqlite3
from flask import Flask, jsonify
from confluent_kafka import Consumer, Producer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
GROUP_ID = os.getenv('GROUP_ID', 'telemetry_group')
REQUEST_TOPIC = os.getenv('REQUEST_TOPIC', 'heating-system-temperature-request')
RESPONSE_TOPIC = os.getenv('RESPONSE_TOPIC', 'heating-system-temperature-response')

app = Flask(__name__)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([REQUEST_TOPIC])


DATABASE = 'telemetry.db'


def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS telemetry (
                        device_id TEXT PRIMARY KEY,
                        telemetry TEXT,
                        timestamp TEXT
                      )''')
    conn.commit()
    conn.close()


init_db()


def save_telemetry_to_db(device_id, telemetry, timestamp):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''INSERT OR REPLACE INTO telemetry (device_id, telemetry, timestamp)
                      VALUES (?, ?, ?)''', (device_id, telemetry, timestamp))
    conn.commit()
    conn.close()


def get_latest_telemetry_from_db(device_id):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT device_id, telemetry, timestamp FROM telemetry WHERE device_id = ?", (device_id,))
    data = cursor.fetchone()
    conn.close()
    return data


def send_temperature_response(device_id, telemetry, timestamp):
    message = {
        "device_id": device_id,
        "telemetry": telemetry,
        "timestamp": timestamp
    }
    producer.produce(RESPONSE_TOPIC, key=device_id, value=json.dumps(message))
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
        device_id = message.get('device_id')

        if device_id:
            telemetry_data = get_latest_telemetry_from_db(device_id)
            if telemetry_data:
                logger.info(f"Sending temperature data for device {device_id}")
                send_temperature_response(telemetry_data[0], telemetry_data[1], telemetry_data[2])
            else:
                logger.warning(f"No telemetry data found for device {device_id}")


consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()


@app.route('/devices/<device_id>/telemetry/latest', methods=['GET'])
def get_latest_telemetry(device_id):
    data = get_latest_telemetry_from_db(device_id)
    if not data:
        return jsonify({"error": "No telemetry data found"}), 404
    return jsonify({
        "device_id": data[0],
        "telemetry": json.loads(data[1]),
        "timestamp": data[2]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5001)))
