import redis
import paho.mqtt.client as mqtt

import json

MQTT_SERVER = "alpha1640"

pool =  redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)

red = redis.Redis(connection_pool=pool)

mqtt_handle = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

feedback_chnls = ["esp1/feedback", "esp2/feedback", "tof/feedback"]
esp32_chnl = {'left':'1', 'right':'2', 'top':'3'}

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe("SYS/")
    for i in feedback_chnls:
        client.subscribe(i)

def on_message(client, userdata, msg):
    if msg.topic == "esp1/feedback":
        data = json.loads(msg.payload)
        if data['status'] == "ERROR":
            red.rename('commands', 'faliure_stack')
        print("An Error occured on channel esp1!")
        print("On-Board feedback:")
        print(data)
    if msg.topic == "SYS/CMD":
        data = json.loads(msg.payload)
        red.rpush('commands', data['command'])
    
        



def main():
    print("Hello from commsintegration!")


if __name__ == "__main__":
    main()
