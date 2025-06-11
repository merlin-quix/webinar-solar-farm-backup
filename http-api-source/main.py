import os
import datetime
import json
from flask import Flask, request, Response, redirect, jsonify
from flasgger import Swagger
from waitress import serve
import time
from typing import Dict, Any, Optional

from flask_cors import CORS

from setup_logging import get_logger
from quixstreams import Application

# Global variable to store the last received data
last_data: Dict[str, Any] = {}
last_key: Optional[str] = None

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

service_url = os.environ["Quix__Deployment__Network__PublicUrl"]

quix_app = Application()
topic = quix_app.topic(os.environ["output"])
producer = quix_app.get_producer()

logger = get_logger()

app = Flask(__name__)

# Enable CORS for all routes and origins by default
CORS(app)

app.config['SWAGGER'] = {
    'title': 'HTTP API Source',
    'description': 'Test your HTTP API with this Swagger interface. Send data and see it arrive in Quix.',
    'uiversion': 3
}

swagger = Swagger(app)

@app.route("/", methods=['GET'])
def redirect_to_swagger():
    return redirect("/apidocs/")

@app.route("/data/", methods=['POST'])
def post_data_without_key():
    """
    Post data without key
    ---
    parameters:
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    
    # Store the last received data
    global last_data, last_key
    last_data = data
    last_key = None  # No key for this endpoint

    producer.produce(topic.name, json.dumps(data))

    return jsonify({"status": "success", "message": "Data received and processed"})

@app.route("/data/<key>", methods=['POST'])
def post_data_with_key(key: str):
    """
    Post data with a key
    ---
    parameters:
      - in: path
        name: key
        type: string
        required: true
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    
    # Store the last received data and key
    global last_data, last_key
    last_data = data
    last_key = key

    producer.produce(topic.name, json.dumps(data), key.encode())

    return jsonify({"status": "success", "message": f"Data with key '{key}' received and processed"})

@app.route("/data/last", methods=['GET'])
def get_last_data():
    """
    Get the last received data
    ---
    responses:
      200:
        description: Last received data
    """
    global last_data, last_key
    if last_key is None:
        return jsonify({"status": "success", "data": last_data})
    else:
        return jsonify({"status": "success", "key": last_key, "data": last_data})

@app.route("/data/resend", methods=['POST'])
def resend_last_data():
    """
    Resend the last received data
    ---
    responses:
      200:
        description: Data resent successfully
    """
    global last_data, last_key
    if last_data and last_key is not None:
        producer.produce(topic.name, json.dumps(last_data), last_key.encode())
        return jsonify({"status": "success", "message": f"Data with key '{last_key}' resent successfully"})
    elif last_data and last_key is None:
        producer.produce(topic.name, json.dumps(last_data))
        return jsonify({"status": "success", "message": "Data resent successfully"})
    else:
        return jsonify({"status": "error", "message": "No last data to resend"})

if __name__ == '__main__':
    print("=" * 60)
    print(" " * 20 + "CURL EXAMPLE")
    print("=" * 60)
    print(
        f"""
curl -L -X POST \\
    -H 'Content-Type: application/json' \\
    -d '{{"key": "value"}}' \\
    {service_url}/data
    """
    )
    print("=" * 60)

    serve(app, host="0.0.0.0", port=80)