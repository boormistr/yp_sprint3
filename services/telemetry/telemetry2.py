import os
import json
import time
import threading
import logging
from flask import Flask, jsonify
from confluent_kafka import Consumer
from datetime import datetime

# Логирование в stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
GROUP_ID = os.getenv('GROUP_ID', 'telemetry_group')
TELEMETRY_TOPIC = os.getenv('TELEMETRY_TOPIC', 'telemetry_data')

app = Flask(__name__)


consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TELEMETRY_TOPIC])

# Хранилище данных телеметрии
telemetry_data = {}


# Обработка сообщений Kafka
def consume_messages():
    time.sleep(5)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        message = json.loads(msg.value().decode('utf-8'))
        device_id = message.get('device_id')
        telemetry = message.get('telemetry')
        timestamp = message.get('timestamp')

        if device_id and telemetry:
            telemetry_data[device_id] = {
                "telemetry": telemetry,
                "timestamp": timestamp
            }
            logger.info(f"Telemetry data updated for device {device_id}: {telemetry}")


consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()


@app.route('/devices/<device_id>/telemetry/latest', methods=['GET'])
def get_latest_telemetry(device_id):
    data = telemetry_data.get(device_id)
    if not data:
        return jsonify({"error": "No telemetry data found"}), 404
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5001)))
