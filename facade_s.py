"""
This script implements the facade pattern,
which is accessed by a user with a POST or GET and, 
depending on the type of request, the required service 
is called - Logging or Messages
"""

import base64
import uuid
import time
import random
import requests
from flask import Flask, request, jsonify, json
import grpc
import logging_pb2
import logging_pb2_grpc
from confluent_kafka import Producer


app = Flask(__name__)


class FacadeService:
    """
    This class implements the Facade pattern
    """

    def __init__(self):
        self.logging_services = self.get_services_from_consul("logging-service")
        self.messages_services = self.get_services_from_consul("messages-service")
        self.kafka_config = self.get_kafka_config()
        if self.kafka_config:
            self.producer = Producer(
                {
                    "bootstrap.servers": self.kafka_config.get(
                        "bootstrap_servers", "localhost:9092"
                    )
                }
            )
        else:

            self.producer = Producer({"bootstrap.servers": "localhost:9092"})

    def acked(self, err, msg):
        """
        Checks message delivering status
        """
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    def send_to_kafka_msg_queue(self, topic, message):
        """
        Sends message to Messages Service via Kafka
        """
        self.producer.produce(topic, value=message.encode("utf-8"), callback=self.acked)
        self.producer.poll(1)

    def get_services_from_consul(self, service_name):
        """
        Function that gets list of services from Consul
        """
        try:
            res = requests.get(
                f"http://localhost:8500/v1/catalog/service/{service_name}"
            )
            if res.status_code == 200:
                services = res.json()
                addresses = [f"{s['Address']}:{s['ServicePort']}" for s in services]
                print(f"Discovered {service_name}: {addresses}")
                return addresses
        except Exception as e:
            print(f"Failed to discover {service_name} from Consul: {e}")
        return []

    def get_kafka_config(self):
        """
        Gets Kafka config form Consul
        """
        try:
            res = requests.get(f"http://localhost:8500/v1/kv/kafka/config")
            if res.status_code == 200:
                raw_value = res.json()[0]["Value"]
                decoded_value = base64.b64decode(raw_value).decode("utf-8")
                return json.loads(decoded_value)
        except Exception as e:
            print(f"Failed to get message queue config from Consul: {e}")
        return None

    def get_random_logging_service(self):
        """
        Gets random Logging service from Cofig Srver to send message
        """
        if not self.logging_services:
            self.logging_services = self.get_services_from_consul("logging-service")
        if not self.logging_services:
            raise Exception("There is not any logging service available")

        selected_service = random.choice(self.logging_services)
        print(f"Selected Logging Service: {selected_service}")
        channel = grpc.insecure_channel(selected_service)
        return logging_pb2_grpc.LoggingServiceStub(channel)

    def get_random_messages_service(self):
        """
        Gets random Messages service from Cofig Srver to send message
        """
        if not self.messages_services:
            self.messages_services = self.get_services_from_consul("messages-service")
        if not self.messages_services:
            raise Exception("There is not any messages service available")

        selected_service = random.choice(self.messages_services)
        print(f"Selected Messages Service: {selected_service}")
        return selected_service

    def save_msg_in_logging_service(self, msg):
        """
        This function implements sending a message to the Logging service.
        """
        msg_uuid = str(uuid.uuid4())
        request_ = logging_pb2.PostMessageRequest(uuid=msg_uuid, msg=msg)
        retries = 5
        attempt = 0

        while attempt < retries:
            try:
                random_service = self.get_random_logging_service()
                print(f"Attempting to send message: {msg} (Attempt {attempt + 1})")
                response = random_service.PostMessage(request_)
                print(f"Response received: {response}")
                return response.success

            except grpc.RpcError as e:
                print(f"gRPC error details: {e}")

                attempt += 1
                if attempt < retries:
                    print(f"Attempt {attempt} failed, retrying...")
                    time.sleep(2)
                else:
                    print(f"Failed after {retries} attempts: {e}")
                    return False

        return False

    def save_msg_in_messaging_service(self, msg):
        """
        Saves message in Messaging service
        """
        try:
            print(f"Sending message to Kafka Messages Queue: {msg}")
            config = self.get_kafka_config()
            if config:
                topic = config.get("topic", "messages-topic-wiwiwi")
            else:
                topic = "messages-topic-wiwiwi"
            self.send_to_kafka_msg_queue(topic, msg)
            return True
        except Exception as e:
            print(f"Failed to send message to Kafka Messages Queue: {e}")
            return False

    def get_all_messages(self):
        """
        This function will ask the Logging and Messages services
        to return a message.
        """
        logging_text = ""
        try:
            random_logging_service = self.get_random_logging_service()
            logging_response = random_logging_service.GetAllMessages(
                logging_pb2.GetMessageRequest()
            )
            logging_text = logging_response.message
        except requests.exceptions.Timeout:
            logging_text = "Timeout: Logging Service dont respond."

        messages_text = ""
        messages = []
        for service in self.messages_services:
            try:
                messages_response = requests.get(
                    f"http://{service}/messages", timeout=2
                )
                service_messages = messages_response.json()
                messages.extend(service_messages)
            except requests.exceptions.Timeout:
                messages_text = "Timeout: Messages Service dont respond."

        return f"{logging_text}\n{json.dumps(messages)}"


facade = FacadeService()


@app.route("/message", methods=["POST"])
def handle_post():
    """
    A function that works with a POST request
    """
    msg = request.json.get("msg")
    if not msg:
        return jsonify({"error": "No message provided"}), 400

    success_logging = facade.save_msg_in_logging_service(msg)
    success_messages = facade.save_msg_in_messaging_service(msg)
    return jsonify(
        {"success_logging": success_logging, "success_messages": success_messages}
    )


@app.route("/messages", methods=["GET"])
def handle_get():
    """
    A function that works with a GET request
    """
    messages = facade.get_all_messages()
    return messages


if __name__ == "__main__":
    print("Starting Facade Service on port 8046")
    app.run(port=8046)
