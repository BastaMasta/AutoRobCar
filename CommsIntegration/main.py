import redis
import paho.mqtt.client as mqtt
import json

pool =  redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)

red = redis.Redis(connection_pool=pool)



def main():
    print("Hello from commsintegration!")


if __name__ == "__main__":
    main()
