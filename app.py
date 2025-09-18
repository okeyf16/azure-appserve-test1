import os
import uuid
import logging 
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient
from dotenv import load_dotenv

load_dotenv()  # Only for local dev

app = Flask(__name__)

# Load config
API_KEY = os.getenv("API_KEY")
TABLE_NAME = "TelemetryData"
conn_str = os.getenv("STORAGE_CONN_STR")
table_client = TableServiceClient.from_connection_string(conn_str).get_table_client(TABLE_NAME)

# Middleware for API key auth
@app.before_request
def check_api_key():
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

# Create new entity
@app.route("/telemetry", methods=["POST"])
def create_entity():
    data = request.json
    entity = {
        "PartitionKey": data.get("deviceId", "unknown"),
        "RowKey": str(uuid.uuid4()),
        **data
    }
    table_client.create_entity(entity)
    return jsonify({"status": "created", "entity": entity}), 201

# Read entities (optionally filter by deviceId)
@app.route("/telemetry", methods=["GET"])
def read_entities():
    try:
        device_id = request.args.get("deviceId")
        
        if device_id:
            # This is the part that is likely failing
            filter_query = f"PartitionKey eq '{device_id}'"
            logging.info(f"Querying with filter: {filter_query}")
            entities = table_client.query_entities(query_filter=filter_query)
        else:
            logging.info("Querying all entities.")
            entities = table_client.list_entities()
            
        return jsonify([e for e in entities]), 200

    except Exception as e:
        # This will catch the crash and log the exact error
        logging.error(f"An error occurred while querying entities: {e}")
        # Return a useful error message instead of crashing
        return jsonify({"error": "Failed to query data from Azure.", "details": str(e)}), 500

# Update entity
@app.route("/telemetry/<row_key>", methods=["PUT"])
def update_entity(row_key):
    data = request.json
    partition_key = data.get("deviceId", "unknown")
    entity = {
        "PartitionKey": partition_key,
        "RowKey": row_key,
        **data
    }
    table_client.update_entity(entity, mode="MERGE")
    return jsonify({"status": "updated", "entity": entity}), 200

# Delete entity
@app.route("/telemetry/<row_key>", methods=["DELETE"])
def delete_entity(row_key):
    partition_key = request.args.get("deviceId", "unknown")
    table_client.delete_entity(partition_key, row_key)
    return jsonify({"status": "deleted"}), 200


if __name__ == "__main__":
    app.run(debug=True)
