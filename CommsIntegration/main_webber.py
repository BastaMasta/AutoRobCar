# HTTP API-based Communication System
# Receives commands via REST API from Flutter/web apps

import datetime as dt
import json
import logging
import os
import threading
import time

import paho.mqtt.client as mqtt
import redis
from dotenv import load_dotenv
from flask import Flask, request, jsonify

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Load configuration
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

max_retries = 10

# Flask app setup
app = Flask(__name__)


# Setup Redis with reconnection logic
def setup_redis_connection():
    """Establish Redis connection with retry logic"""
    retry_count = 0
    max_redis_retries = 10
    
    while retry_count < max_redis_retries:
        try:
            print(f"Attempting to connect to Redis at {REDIS_SERVER}:{REDIS_PORT}...")
            logger.info(f"Attempting Redis connection (attempt {retry_count + 1}/{max_redis_retries})")
            
            pool = redis.ConnectionPool(
                host=REDIS_SERVER, 
                port=REDIS_PORT, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            red = redis.Redis(connection_pool=pool)
            
            # Test the connection
            red.ping()
            
            print(f"✓ Successfully connected to Redis at {REDIS_SERVER}:{REDIS_PORT}")
            logger.info(f"Redis connection established successfully at {REDIS_SERVER}:{REDIS_PORT}")
            
            return red
            
        except redis.ConnectionError as e:
            retry_count += 1
            print(f"✗ Failed to connect to Redis (attempt {retry_count}/{max_redis_retries}): {e}")
            logger.error(f"Redis connection failed (attempt {retry_count}/{max_redis_retries}): {e}")
            
            if retry_count < max_redis_retries:
                wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30 seconds
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"✗ Failed to connect to Redis after {max_redis_retries} attempts. Exiting.")
                logger.critical(f"Redis connection failed after {max_redis_retries} attempts")
                raise
        
        except Exception as e:
            print(f"✗ Unexpected error connecting to Redis: {e}")
            logger.error(f"Unexpected Redis connection error: {e}")
            raise


# Initialize Redis
red = setup_redis_connection()


def check_redis_connection(red_instance):
    """Check if Redis is still connected"""
    try:
        red_instance.ping()
        return True
    except redis.ConnectionError:
        print("✗ Lost connection to Redis. Attempting to reconnect...")
        logger.warning("Redis connection lost, attempting reconnection")
        return False


# Setup MQTT (still needed for publishing to ESP devices)
mqtt_handle = mqtt.Client(
    client_id="DataPusher_HTTP",
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    protocol=mqtt.MQTTv5,
)

mqtt_handle.username_pw_set(os.getenv("MQTT_USER"), os.getenv("MQTT_PASS"))


def clr_queue():
    """Clears the queue in case of failure"""
    red.rename("commands", "faliure_stack")


# MQTT callbacks for ESP device feedback (errors, sensor data)
def on_connect(client, userdata, flags, reason_code, properties):
    """MQTT connection callback"""
    print(f"✓ Connected to MQTT broker with result code {reason_code}")
    logger.info(f"MQTT connected with result code {reason_code}")
    
    # Only subscribe to feedback topics, NOT command topics
    for i in ERROR_TOPICS:
        client.subscribe(i)
    for i in SENSE_TOPICS:
        client.subscribe(i)
    
    print("✓ Subscribed to MQTT feedback topics")
    logger.info("Subscribed to MQTT error and sensor topics")


def on_message(client, userdata, msg):
    """MQTT message callback - handles ESP device feedback"""
    if msg.topic in ERROR_TOPICS:
        data = json.loads(msg.payload)
        if data["status"] == "ERROR":
            clr_queue()
            print(f"✗ Error occurred on topic {msg.topic}!")
            print(f"On-Board feedback: {data}")
            logger.error(f"Error occurred on topic {msg.topic}!")
            logger.error(f"On-Board feedback:\n{msg}")
            
            time.sleep(1)
            print("Sending incomplete progress feedback to parent process...")
            red.rpush("faliure_stack", str(msg))
            mqtt_handle.publish("SYS/ERR", json.dumps(red.lrange("faliure_stack", 0, -1)), qos=1)
            red.rename("faliure_stack", f"error_{dt.datetime.now()}")
    
    # Log sensor data if needed
    if msg.topic in SENSE_TOPICS:
        data = json.loads(msg.payload)
        logger.info(f"Sensor data received from {msg.topic}: {data}")


# HTTP API Endpoints
@app.route('/app_cmd', methods=['POST'])
def flutter_cmd():
    """
    Receive commands from Flutter/HTTP clients
    Expected JSON: {"cmd": "forward"} or {"topic": "SYS/CMD", "body": {...}}
    """
    try:
        data = request.json
        
        # Validate input
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400
        
        # Support two formats:
        # 1. Simple: {"cmd": "forward"}
        # 2. Full: {"topic": "SYS/CMD", "body": {"cmd": "forward"}}
        
        if 'cmd' in data:
            # Simple format - convert to full format
            cmd_data = {
                "topic": "SYS/CMD",
                "body": {"cmd": data['cmd']}
            }
        elif 'topic' in data and 'body' in data:
            # Full format already
            cmd_data = data
        else:
            return jsonify({
                "status": "error", 
                "message": "Invalid format. Provide either 'cmd' or 'topic'+'body'"
            }), 400
        
        # Push to Redis queue (same queue as MQTT version!)
        red.rpush("commands", json.dumps(cmd_data))
        queue_length = red.llen("commands")
        
        logger.info(f"HTTP command queued: {cmd_data}, queue position: {queue_length}")
        print(f"✓ Command queued from HTTP: {cmd_data}")
        
        return jsonify({
            "status": "queued",
            "command": cmd_data,
            "queue_position": queue_length,
            "timestamp": str(dt.datetime.now())
        }), 200
        
    except redis.ConnectionError:
        logger.error("Redis connection lost during HTTP request")
        return jsonify({"status": "error", "message": "Database unavailable"}), 503
        
    except json.JSONDecodeError:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400
        
    except Exception as e:
        logger.error(f"Error processing HTTP command: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        red.ping()
        mqtt_connected = mqtt_handle.is_connected()
        
        return jsonify({
            "status": "healthy",
            "redis_connected": True,
            "mqtt_connected": mqtt_connected,
            "queue_length": red.llen("commands"),
            "processing_queue_length": red.llen("processing"),
            "timestamp": str(dt.datetime.now())
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503


@app.route('/queue_status', methods=['GET'])
def queue_status():
    """Get detailed queue status"""
    try:
        return jsonify({
            "commands_pending": red.llen("commands"),
            "commands_processing": red.llen("processing"),
            "timestamp": str(dt.datetime.now())
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def process_queue():
    """Process commands from Redis queue and publish to MQTT"""
    global red
    
    while True:
        comm = red.brpoplpush("commands", "processing", timeout=3)

        if comm is None:
            continue

        command = json.loads(comm)
        try:
            logger.info(f"Broadcasting message to subordinate ESP: {command['topic']}")
            mqtt_handle.publish(command["topic"], command["body"])
            red.lrem("processing", 1, comm)  # Remove from processing queue
            
        except Exception as e:
            logger.error(
                f"Error broadcasting command: {e}\nRetrying after timeout..."
            )
            time.sleep(1)
            err_cnt = 1
            
            while err_cnt <= max_retries:
                try:
                    logger.info("Retrying broadcast to subordinate ESP...")
                    mqtt_handle.publish(command["topic"], command["body"])
                    logger.info("Successfully published after retry!")
                    red.lrem("processing", 1, comm)
                    break

                except Exception as e_retry:
                    err_cnt += 1
                    if err_cnt > max_retries:
                        logger.critical(
                            f"Failed after {max_retries} retries. Moving to dead letter queue. Error: {e_retry}"
                        )
                        clr_queue()
                        print("Sending incomplete progress feedback to parent process...")

                        red.rpush("faliure_stack", str(comm))

                        failure_report = {
                            "status": "SEQUENCE_FAILED",
                            "failed_command": command,
                            "error": str(e_retry),
                            "timestamp": str(dt.datetime.now()),
                        }

                        mqtt_handle.publish(
                            "SYS/ERR",
                            json.dumps(failure_report),
                            qos=1,
                        )
                        red.rename("faliure_stack", f"error_{dt.datetime.now()}")
                        break

                    logger.error(
                        f"Retry {err_cnt}/{max_retries} failed: {e_retry}"
                    )
                    time.sleep(0.5 * err_cnt)


def run_flask():
    """Run Flask server"""
    print("✓ Starting HTTP API server on 0.0.0.0:5000")
    logger.info("Flask HTTP server starting on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


def main():
    global red
    
    print("=" * 60)
    print("HTTP API-Based Communication System Starting...")
    print("=" * 60)
    
    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Connect to MQTT for sending commands and receiving feedback
    print(f"Connecting to MQTT broker at {MQTT_SERVER}:{MQTT_PORT}...")
    mqtt_handle.on_connect = on_connect
    mqtt_handle.on_message = on_message
    mqtt_handle.connect(MQTT_SERVER, int(MQTT_PORT))
    mqtt_handle.loop_start()

    # Start command processing thread
    queue_thread = threading.Thread(target=process_queue, daemon=True)
    queue_thread.start()
    print("✓ Command processing thread started")
    
    print("\n" + "=" * 60)
    print("System Ready!")
    print("HTTP API available at: http://0.0.0.0:5000/app_cmd")
    print("Health check at: http://0.0.0.0:5000/health")
    print("=" * 60 + "\n")

    try:
        health_check_counter = 0
        while True:
            time.sleep(1)
            
            # Check Redis health every 30 seconds
            health_check_counter += 1
            if health_check_counter >= 30:
                if not check_redis_connection(red):
                    red = setup_redis_connection()
                health_check_counter = 0
                
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("Shutting down HTTP API system...")
        print("=" * 60)
        mqtt_handle.loop_stop()
        logger.info("HTTP API system shutdown complete")


if __name__ == "__main__":
    main()