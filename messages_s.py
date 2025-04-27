"""
This script implements a server Messaging that returns a static message
"""

import base64
import threading
import sys
import argparse
import json
import time
from threading import Thread
from flask import Flask, jsonify
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError

app = Flask(__name__)


@app.route("/health")
def health():
    """
    Health port responce
    """
    return "OK", 200


@app.route("/messages", methods=["GET"])
def get_messages():
    """
    A function that works with a GET request
    """
    return jsonify(MESSAGES)


def run_flask(port):
    app.run(host="0.0.0.0", port=port)


MESSAGES = []


def parse_arguments():
    """
    This function parses command line arguments to get
    the port on which to run the service.
    """
    parser = argparse.ArgumentParser(description="Start a logging service server")
    parser.add_argument(
        "--port", type=int, required=True, help="Port number to run the server on"
    )
    parser.add_argument(
        "--health-port",
        type=int,
        required=True,
        help="Port number for the health check",
    )
    return parser.parse_args()


args = parse_arguments()
PORT = args.port
HEALTH_PORT = args.health_port


def kafka_consumer():
    """
    function with Kafka consumer logic
    """
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "messages-service-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "session.timeout.ms": 6000,
            "max.poll.interval.ms": 300000,
        }
    )

    consumer.subscribe(["messages-topic-wiwiwi"])
    print(f"Messages Service on {PORT} is listening to Kafka Messages Queue...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                MESSAGES.append(msg.value().decode("utf-8"))
                print(
                    f"Kafka Messages Queue Received message: {msg.value().decode('utf-8')}"
                )
    finally:
        consumer.close()


def register_service_in_consul(service_name, service_port, health_port):
    """
    Function to register service in consul
    """
    hostname = "host.docker.internal"
    registration = {
        "ID": f"{service_name}-{service_port}",
        "Name": service_name,
        "Address": hostname,
        "Port": service_port,
        "Check": {
            "HTTP": f"http://{hostname}:{health_port}/health",
            "Interval": "10s",
        },
    }
    try:
        res = requests.put(
            f"http://localhost:8500/v1/agent/service/register", json=registration
        )
        print(f"Registered {service_name} with Consul: {res.status_code}")
    except Exception as e:
        print(f"Failed to register {service_name} with Consul: {e}")


def save_to_consul(config_data):
    """
    Function that saves Kafka configuration in Consul
    """
    consul_url = f"http://localhost:8500/v1/kv/kafka/config"
    try:
        response = requests.put(consul_url, data=json.dumps(config_data))
        if response.status_code == 200:
            print("Kafka configuration saved to Consul successfully.")
        else:
            print(f"Failed to save Kafka config to Consul: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error saving to Consul: {e}")


def get_from_consul():
    """
    Function that gets Kafka configuration from Consul
    """
    consul_url = "http://localhost:8500/v1/kv/kafka/config"
    try:
        response = requests.get(consul_url)
        if response.status_code == 200:
            config_data_encoded = response.json()[0]["Value"]
            config_json = json.loads(
                base64.b64decode(config_data_encoded).decode("utf-8")
            )
            print("Kafka config loaded from Consul:", config_json)
            return config_json
        else:
            print(f"Failed to fetch Kafka config: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Kafka config: {e}")
        return None


if __name__ == "__main__":
    flask_health_thread = Thread(target=run_flask, args=(HEALTH_PORT,))
    flask_health_thread.daemon = True
    flask_health_thread.start()

    print(f"Starting Messages Service on port {PORT}...")

    kafka_thread = threading.Thread(target=kafka_consumer, daemon=True)
    kafka_thread.start()

    kafka_config = {"topic": "messages-topic-wiwiwi", "port": "9092", "partitions": "3"}
    save_to_consul(kafka_config)
    time.sleep(1)
    print("Saved to Consul succesfully")

    config_from_consul = get_from_consul()
    if not config_from_consul:
        raise RuntimeError("Could not fetch Kafka config from Consul")

    register_service_in_consul("messages-service", PORT, HEALTH_PORT)

    app.run(host="0.0.0.0", port=PORT)
