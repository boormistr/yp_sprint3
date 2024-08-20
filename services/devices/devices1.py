from flask import Flask
from confluent_kafka import Producer
import json
import time
import threading
from datetime import datetime

app = Flask(__name__)


producer_config = {
    'bootstrap.servers': 'kafka:9092'
}
producer = Producer(producer_config)


def send_messages():
    time.sleep(15)
    while True:
        try:
            current_time = datetime.now().strftime('%H:%M:%S')
            message = f"Время на часах {current_time}"
            print(f"Готовим к отправке: {message}", flush=True)
            producer.produce('flask_topic', key='time_key', value=json.dumps({"message": message}))
            producer.flush()
            print(f"Продюсер отправил сообщение: {message}", flush=True)
        except Exception as e:
            print(f"Ошибка при отправке сообщения: {e}", flush=True)
        time.sleep(5)


producer_thread = threading.Thread(target=send_messages)
producer_thread.daemon = True
producer_thread.start()


@app.route('/')
def home():
    return "Producer is sending messages every 5 seconds."


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
