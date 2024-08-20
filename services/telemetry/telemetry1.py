import time

from flask import Flask, jsonify
from confluent_kafka import Consumer
import threading
import json

app = Flask(__name__)

consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flask_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['flask_topic'])

messages = []


def consume_messages():
    time.sleep(15)
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Kafka: {msg.error()}", flush=True)
            else:
                try:
                    message_text = json.loads(msg.value().decode('utf-8'))['message']
                    print(f"Я получил сообщение: {message_text}", flush=True)
                    messages.append(message_text)
                except json.JSONDecodeError as e:
                    print(f"Ошибка декодирования JSON: {e} - Сообщение: {msg.value()}", flush=True)
        except Exception as e:
            print(f"Ошибка при получении сообщения: {e}", flush=True)


consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()


@app.route('/')
def home():
    return jsonify(messages)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
