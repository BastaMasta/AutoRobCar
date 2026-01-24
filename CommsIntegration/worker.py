# Consumes Redis queue and broadcasts to edge devices

import json

import paho.mqtt.client as mqtt
import redis

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

def on_message(client, userdata, msg):
    if msg.topic in ERROR_CHANNELS:
        data = json.loads(msg.payload)
        if data["status"] == "ERROR":
            clr_queue()
        print("An Error occured on channel esp1!")
        print("On-Board feedback:")
        print(data)