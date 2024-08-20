import os
import json
import time
import threading
import logging
from flask import Flask, request, jsonify
from confluent_kafka import Consumer, Producer
from datetime import datetime


# Логирование в stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# Конфигурация через переменные окружения
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
DEVICE_TOPIC = os.getenv('DEVICE_TOPIC', 'device_updates')
COMMAND_TOPIC = os.getenv('COMMAND_TOPIC', 'device_commands')

app = Flask(__name__)

# Kafka Producer и Consumer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'device_management_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([DEVICE_TOPIC])


# Хранилище состояния устройств (временное)
device_status = {}


# Обработка сообщений Kafka
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
        event = message.get('event')
        device_id = message.get('device_id')

        if event == 'status_update':
            status = message.get('status')
            logger.info(f"Received status update for device {device_id}: {status}")
            device_status[device_id] = status
        elif event == 'command':
            command = message.get('command')
            logger.info(f"Executing command {command} for device {device_id}")


consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()


# Эндпойнты управления устройствами (например, для ручного вмешательства)
@app.route('/devices/<device_id>/status', methods=['GET'])
def get_device_status(device_id):
    status = device_status.get(device_id, "unknown")
    return jsonify({"device_id": device_id, "status": status})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
