# Communicates with parent, and populates the

import datetime
import json
import logging
import os
import time

import paho.mqtt.client as mqtt
import redis
import sentry_sdk
from dotenv import load_dotenv

load_dotenv()

# Set up sentry for loggin and profiling
sentry_sdk.init(
    dsn=os.getenv("SENTRY_URL"),
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    # Enable sending logs to Sentry
    enable_logs=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    # Set profile_session_sample_rate to 1.0 to profile 100%
    # of profile sessions.
    profile_session_sample_rate=1.0,
)

logger = logging.getLogger(__name__)

# Defining constants and channels
jdata = json.load(open("server_data.json", "r", encoding="utf-8"))

MQTT_SERVER = jdata["mqtt_server"]
MQTT_PORT = jdata["mqtt_port"]
REDIS_SERVER = jdata["red_server"]
REDIS_PORT = jdata["red_port"]

CMD_CHANNELS = jdata["command_channels"]
ERROR_CHANNELS = jdata["error_channels"]
SENSE_CHANNELS = jdata["sense_channels"]

PARENT_CHANNEL = jdata["parent_channel"]

del jdata

# Setup Redis
pool = redis.ConnectionPool(host=REDIS_SERVER, port=REDIS_PORT, decode_responses=True)
red = redis.Redis(connection_pool=pool)

# Setup MQTT
mqtt_handle = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)


# Clears the queue in case of faliure
def clr_queue():
    red.rename("commands", "faliure_stack")


# Resolve the commands recieved from parent
def resolve_cmd(data) -> str:
    return "pass"


# Start subscriptions
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe("SYS/")
    for i in ERROR_CHANNELS:
        client.subscribe(i)
    for i in SENSE_CHANNELS:
        client.subscribe(i)


def on_message(client, userdata, msg):
    if msg.topic in ERROR_CHANNELS:
        data = json.loads(msg.payload)
        if data["status"] == "ERROR":
            clr_queue()
            print("An Error occured on channel esp1!")
            print("On-Board feedback:")
            print(data)
            logger.error("An Error occured on channel esp1!")
            logger.error(f"On-Board feedback:\n{msg}")
            time.sleep(1)
            print("Sending incomplete progress feedback to parent process...")
            red.rpush("faliure_stack", str(msg))
            client.publish("SYS/ERR", red.lrange("faliure_stack", 0, -1), qos=1)
            red.rename("faliure_stack", f"error_{datetime.datetime.now()}")

    if msg.topic == "SYS/CMD":
        data = json.loads(msg.payload)
        red.rpush("commands", resolve_cmd(data))


def main():
    print("Hello from commsintegration!")


sentry_sdk.profiler.start_profiler()
if __name__ == "__main__":
    main()
sentry_sdk.profiler.stop_profiler()
