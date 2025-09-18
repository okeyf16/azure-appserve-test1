import os
import uuid
import logging
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient
from dotenv import load_dotenv
from opencensus.ext.azure.log_exporter import AzureLogHandler

# Load environment variables (for local dev)
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Use minimal connection string for Application Insights
app_insights_conn_str = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
if app_insights_conn_str:
    logger.addHandler(AzureLogHandler(connection_string=app_insights_conn_str))
    logger.info("Application Insights logging initialized")

# Load config values
API_KEY = os.getenv("API_KEY")
TABLE_NAME = "TelemetryData"
conn_str = os.getenv("STORAGE_CONN_STR")
table_client = TableServiceClient.from_connection_string(conn_str).get_table_client(TABLE_NAME)

# Middleware for API key authentication
@app.before_request
def check_api_key():
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        logger.warning("Unauthorized access attempt")
        return jsonify({"error": "Unauthorized"}), 401

# Create new telemetry entity
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
        logger.info(f"Created entity for deviceId: {entity['PartitionKey']}")
        return jsonify({"status": "created", "entity": entity}), 201
    except Exception as e:
        logger.error(f"Failed to create entity: {e}")
        return jsonify({"error": "Failed to create entity", "details": str(e)}), 500

# Read telemetry entities (optionally filtered by deviceId)
@app.route("/telemetry", methods=["GET"])
def read_entities():
    try:
        device_id = request.args.get("deviceId")
        if device_id:
            filter_query = f"PartitionKey eq '{device_id}'"
            logger.info(f"Querying with filter: {filter_query}")
            entities = table_client.query_entities(query_filter=filter_query)
        else:
            logger.info("Querying all entities")
            entities = table_client.list_entities()

        return jsonify([e for e in entities]), 200
    except Exception as e:
        logger.error(f"Error querying entities: {e}")
        return jsonify({"error": "Failed to query data", "details": str(e)}), 500

# Update telemetry entity
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
        logger.info(f"Updated entity {row_key} for deviceId: {partition_key}")
        return jsonify({"status": "updated", "entity": entity}), 200
    except Exception as e:
        logger.error(f"Failed to update entity: {e}")
        return jsonify({"error": "Failed to update entity", "details": str(e)}), 500

# Delete telemetry entity
@app.route("/telemetry/<row_key>", methods=["DELETE"])
def delete_entity(row_key):
    try:
        partition_key = request.args.get("deviceId", "unknown")
        table_client.delete_entity(partition_key, row_key)
        logger.info(f"Deleted entity {row_key} for deviceId: {partition_key}")
        return jsonify({"status": "deleted"}), 200
    except Exception as e:
        logger.error(f"Failed to delete entity: {e}")
        return jsonify({"error": "Failed to delete entity", "details": str(e)}), 500

# Run the app
if __name__ == "__main__":
    logger.info("Starting Flask app")
    app.run(debug=True)
