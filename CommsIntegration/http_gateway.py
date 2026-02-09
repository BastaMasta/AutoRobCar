import json
import logging

import redis
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

# Load same config as main.py
jdata = json.load(open("server_data.json", "r", encoding="utf-8"))
REDIS_SERVER = jdata["red_server"]
REDIS_PORT = jdata["red_port"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


# Reuse your Redis connection logic
def setup_redis():
    try:
        pool = redis.ConnectionPool(
            host=REDIS_SERVER,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        red = redis.Redis(connection_pool=pool)
        red.ping()
        print(f"✓ HTTP Gateway connected to Redis at {REDIS_SERVER}:{REDIS_PORT}")
        logger.info("HTTP Gateway Redis connection established")
        return red
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        logger.error(f"Redis connection failed: {e}")
        raise


red = setup_redis()


@app.route("/app_cmd", methods=["POST"])
def flutter_cmd():
    try:
        data = request.json

        # Validate input
        if not data or "cmd" not in data:
            return jsonify({"status": "error", "message": "Missing 'cmd' field"}), 400

        # Format matches your resolve_cmd structure
        cmd_data = {"topic": "SYS/CMD", "body": {"cmd": data["cmd"]}}

        # Push to YOUR existing queue
        red.rpush("commands", json.dumps(cmd_data))
        queue_length = red.llen("commands")

        logger.info(
            f"HTTP command queued: {data['cmd']}, queue position: {queue_length}"
        )

        return jsonify(
            {"status": "queued", "command": data["cmd"], "position": queue_length}
        ), 200

    except redis.ConnectionError:
        logger.error("Redis connection lost during HTTP request")
        return jsonify({"status": "error", "message": "Database unavailable"}), 503

    except Exception as e:
        logger.error(f"Error processing HTTP command: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    try:
        red.ping()
        return jsonify({"status": "healthy", "queue_length": red.llen("commands")}), 200
    except:
        return jsonify({"status": "unhealthy"}), 503


if __name__ == "__main__":
    print("Starting HTTP Gateway on port 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False)
