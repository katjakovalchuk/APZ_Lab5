"""
this Python script implements a login service to accept a pair of 
{uuid, message} and save them to a local hash table, 
where the key is the hash of the message.
"""

import asyncio
import base64
import hashlib
import argparse
import grpc
from threading import Thread
import json
import time
import logging_pb2
import logging_pb2_grpc
import hazelcast
import requests
from flask import Flask


app = Flask(__name__)


@app.route("/health")
def health():
    """
    Health port responce
    """
    return "OK", 200


def run_flask(port):
    app.run(host="0.0.0.0", port=port)


class MessageStorage:
    """
    This class is used to store notifications that we receive
    """

    def __init__(self, hazelcast_client):
        self.client = hazelcast_client
        self.message_map = self.client.get_map("messages").blocking()

    def save_message(self, uuid, message):
        """
        This function is designed to store
        messages that we receive in a pair {hash, message}.
        """
        message_hash = hashlib.sha256(f"{message}".encode("utf-8")).hexdigest()

        if not self.message_map.contains_key(message_hash):
            self.message_map.put(message_hash, message)
            return True
        else:
            return False

    def get_all_messages(self):
        """
        Function returns all messages from the message_table
        """
        return " ".join(self.message_map.values())


class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    """
    This class implements the Logging service that accepts
    messages and saves them or returns them.
    """

    def __init__(self, hazelcast_client):
        self.message_storage = MessageStorage(hazelcast_client)

    async def PostMessage(self, request, context):
        """
        An asynchronous function that saves messages in a
        hazelcast map and returns True if the message was saved successfully,
        False if not.
        """
        success = self.message_storage.save_message(request.uuid, request.msg)
        if success:
            print(f"Message received: {request.msg}")
        return logging_pb2.PostMessageSuccess(success=True)

    async def GetAllMessages(self, request, context):
        """
        An asynchronous function that returns all messages from a hazelcast map.
        """
        messages = self.message_storage.get_all_messages()
        return logging_pb2.GetMessageResponce(message=messages)


# async def register_service_in_config_server(port):
#     """
#     This function makes a request to the Config Server to register the service
#     """
#     hostname = socket.gethostbyname(socket.gethostname())
#     service_data = {"service_name": "logging-service", "ip": f"{hostname}:{port}"}

#     try:
#         await asyncio.to_thread(
#             lambda: requests.post("http://localhost:5000/register", json=service_data)
#         )
#         print("Service registered successfully")
#     except requests.exceptions.RequestException as e:
#         print(f"Failed to register logging-service: {e}")


async def register_with_consul(service_name, grpc_port, health_port):
    """
    Function to register service in consul
    """
    hostname = "host.docker.internal"
    registration = {
        "ID": f"{service_name}-{grpc_port}",
        "Name": service_name,
        "Address": hostname,
        "Port": grpc_port,
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


def save_to_consul(config_data, port):
    """
    Function that saves Hazelcast configuration in Consul
    """
    consul_url = f"http://localhost:8500/v1/kv/hazelcast/{port}/config"
    try:
        response = requests.put(consul_url, data=json.dumps(config_data))
        if response.status_code == 200:
            print("Hazelcast configuration saved to Consul successfully.")
        else:
            print(f"Failed to save Hazelcast config to Consul: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error saving to Consul: {e}")


def get_from_consul(port):
    """
    Function that gets Kafka configuration from Consul
    """
    consul_url = f"http://localhost:8500/v1/kv/hazelcast/{port}/config"
    try:
        response = requests.get(consul_url)
        if response.status_code == 200:
            config_data_encoded = response.json()[0]["Value"]
            config_json = json.loads(
                base64.b64decode(config_data_encoded).decode("utf-8")
            )
            print("Hazelcast config loaded from Consul:", config_json)
            return config_json
        else:
            print(f"Failed to fetch Hazelcast config: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Hazelcast config: {e}")
        return None


async def run_server(port, health_port):
    """
    An asynchronous function that starts and runs
    the server until we stop it with Ctrl+Z.
    """
    flask_thread = Thread(target=run_flask, args=(health_port,))
    flask_thread.daemon = True
    flask_thread.start()

    print(f"Starting gRPC server on port {port}...")

    hazelcast_config = {"name": "hello-world", "ip": "127.0.0.1", "port": f"{port}"}
    save_to_consul(hazelcast_config, port)
    time.sleep(1)
    print("Saved to Consul succesfully")

    config_from_consul = get_from_consul(port)
    if config_from_consul:
        hazelcast_client = hazelcast.HazelcastClient(
            cluster_name=config_from_consul.get("name", "default"),
        )
    else:
        raise RuntimeError("Failed to load Hazelcast config from Consul.")

    print("Hazelcast client initialized successfully")

    await register_with_consul("logging-service", port, health_port)

    server = grpc.aio.server()
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(
        LoggingService(hazelcast_client), server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")

    await server.start()
    print(f"Server started on {port}, waiting for connections...")
    await server.wait_for_termination()


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
        help="Port number for the health check endpoint",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    asyncio.run(run_server(args.port, args.health_port))
