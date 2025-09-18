import os
import uuid
import logging
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient
from azure.core.exceptions import AzureError  # generic Azure SDK error
from dotenv import load_dotenv

# Load environment variables for local dev
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load config
API_KEY = os.getenv("API_KEY")
TABLE_NAME = "TelemetryData"
conn_str = os.getenv("STORAGE_CONN_STR")

# Create table client
table_client = TableServiceClient.from_connection_string(conn_str).get_table_client(TABLE_NAME)

# Middleware for API key auth
@app.before_request
def check_api_key():
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        logging.warning("Unauthorized request")
        return jsonify({"error": "Unauthorized"}), 401

# Create new entity
@app.route("/telemetry", methods=["POST"])
def create_entity():
    try:
        data = request.json
        entity = {
            "PartitionKey": data.get("deviceId", "unknown"),
            "RowKey": str(uuid.uuid4()),
            **data
        }
        table_client.create_entity(entity)
        logging.info(f"Created entity for {entity['PartitionKey']}")
        return jsonify({"status": "created", "entity": entity}), 201
    except AzureError as e:
        logging.error(f"Azure error creating entity: {e}")
        return jsonify({"error": "Azure error", "details": str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Unexpected error", "details": str(e)}), 500

# Read entities
@app.route("/telemetry", methods=["GET"])
def read_entities():
    try:
        device_id = request.args.get("deviceId")
        if device_id:
            filter_query = f"PartitionKey eq '{device_id}'"
            logging.info(f"Querying with filter: {filter_query}")
            entities = table_client.query_entities(query_filter=filter_query)
        else:
            logging.info("Querying all entities")
            entities = table_client.list_entities()

        return jsonify([e for e in entities]), 200
    except AzureError as e:
        logging.error(f"Azure error reading entities: {e}")
        return jsonify({"error": "Azure error", "details": str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Unexpected error", "details": str(e)}), 500

# Update entity
@app.route("/telemetry/<row_key>", methods=["PUT"])
def update_entity(row_key):
    try:
        data = request.json
        partition_key = data.get("deviceId", "unknown")
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            **data
        }
        table_client.update_entity(entity, mode="MERGE")
        logging.info(f"Updated entity {row_key} for {partition_key}")
        return jsonify({"status": "updated", "entity": entity}), 200
    except AzureError as e:
        logging.error(f"Azure error updating entity: {e}")
        return jsonify({"error": "Azure error", "details": str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Unexpected error", "details": str(e)}), 500

# Delete entity
@app.route("/telemetry/<row_key>", methods=["DELETE"])
def delete_entity(row_key):
    try:
        partition_key = request.args.get("deviceId", "unknown")
        table_client.delete_entity(partition_key, row_key)
        logging.info(f"Deleted entity {row_key} for {partition_key}")
        return jsonify({"status": "deleted"}), 200
    except AzureError as e:
        logging.error(f"Azure error deleting entity: {e}")
        return jsonify({"error": "Azure error", "details": str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Unexpected error", "details": str(e)}), 500

# Local dev entrypoint
if __name__ == "__main__":
    app.run(debug=True)
