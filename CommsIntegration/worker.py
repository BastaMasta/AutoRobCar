# Consumes Redis queue and broadcasts to edge devices

import json
import logging
import time

import paho.mqtt.client as mqtt
import redis

logger = logging.getLogger("worker_zbs")

# Defining constants and topics
jdata = json.load(open("server_data.json", "r", encoding="utf-8"))

MQTT_SERVER = jdata["mqtt_server"]
MQTT_PORT = jdata["mqtt_port"]
REDIS_SERVER = jdata["red_server"]
REDIS_PORT = jdata["red_port"]

CMD_TOPICS = jdata["command_topics"]
ERROR_TOPICS = jdata["error_topics"]
SENSE_TOPICS = jdata["sense_topics"]

PARENT_TOPIC = jdata["parent_topic"]

del jdata

# Setup Redis
pool = redis.ConnectionPool(host=REDIS_SERVER, port=REDIS_PORT, decode_responses=True)
red = redis.Redis(connection_pool=pool)

# Setup MQTT
mqtt_handle = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

def on_connect(client, userdata, flags ,rc):
    global flag_connected
    flag_connected = 1

def on_message(client, userdata, msg):
    if msg.topic in ERROR_TOPICS:
        data = json.loads(msg.payload)
        if data["status"] == "ERROR":
            print("An Error occured on topic esp1!")
            print("sleeping for 2 seconds to let system recover")
            time.sleep(2)

def on_disconnect(client, userdata, rc):
   global flag_connected
   flag_connected = 0




while True:
    pass
