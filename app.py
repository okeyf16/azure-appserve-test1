import os
import uuid
import logging
from typing import Optional
from flask import Flask, request, jsonify

# from opencensus.ext.azure.log_exporter import AzureLogHandler # New app insights addition #
# External deps imported inside functions when needed to avoid import-time crashes
# from azure.data.tables import TableServiceClient
# from azure.core.exceptions import AzureError

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telemetry_api")

# --- Application Insights via OpenTelemetry ---
from azure.monitor.opentelemetry import configure_azure_monitor
ai_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
if ai_conn:
    configure_azure_monitor(connection_string=ai_conn)
    logger.info("Application Insights (OpenTelemetry) enabled")
# ----------------------------------------------

# Config (read env, but do not connect yet)
API_KEY = os.getenv("API_KEY")
TABLE_NAME = os.getenv("TABLE_NAME", "TelemetryData")
STORAGE_CONN_STR = os.getenv("STORAGE_CONN_STR")

# Lazy singletons
_table_client = None
_sdk_loaded = None  # tri-state: None unknown, True loaded, False not installed


def load_sdk():
    global _sdk_loaded, TableServiceClient, AzureError
    if _sdk_loaded is not None:
        return _sdk_loaded
    try:
        from azure.data.tables import TableServiceClient  # type: ignore
        from azure.core.exceptions import AzureError  # type: ignore
        globals()["TableServiceClient"] = TableServiceClient
        globals()["AzureError"] = AzureError
        _sdk_loaded = True
        logger.info("Azure SDK loaded")
    except Exception as e:
        _sdk_loaded = False
        logger.error("Azure SDK not available: %s", e)
    return _sdk_loaded


def get_table_client():
    global _table_client
    if _table_client is not None:
        return _table_client

    if not load_sdk():
        return None

    if not STORAGE_CONN_STR:
        logger.error("STORAGE_CONN_STR not set")
        return None

    try:
        svc = TableServiceClient.from_connection_string(STORAGE_CONN_STR)  # type: ignore
        _table_client = svc.get_table_client(TABLE_NAME)  # type: ignore
        logger.info("Connected to Table Storage table '%s'", TABLE_NAME)
        return _table_client
    except Exception as e:
        logger.error("Failed to create Table client: %s", e)
        return None


@app.before_request
def check_api_key():
    # Allow health and root without key
    if request.path in ("/", "/healthz"):
        return None
    key = request.headers.get("x-api-key")
    if not API_KEY or key != API_KEY:
        logger.warning("Unauthorized request to %s from %s", request.path, request.remote_addr)
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "name": "IoT Telemetry API",
        "endpoints": ["/healthz", "/telemetry"],
        "table": TABLE_NAME
    }), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    tc = get_table_client()
    return jsonify({
        "ok": bool(tc and API_KEY),
        "details": {
            "sdk_loaded": bool(_sdk_loaded),
            "api_key_set": bool(API_KEY),
            "storage_conn_str_set": bool(STORAGE_CONN_STR),
            "table_client_ready": bool(tc)
        }
    }), 200 if (tc and API_KEY) else 500


@app.route("/telemetry", methods=["POST"])
def create_entity():
    tc = get_table_client()
    if tc is None:
        return jsonify({"error": "Storage not configured or SDK missing"}), 500
    try:
        data = request.get_json(force=True) or {}
        partition = data.get("deviceId", "unknown")
        entity = {
            "PartitionKey": partition,
            "RowKey": str(uuid.uuid4()),
            **data
        }
        tc.create_entity(entity)
        logger.info("Created entity for device %s", partition)
        return jsonify({"status": "created", "entity": entity}), 201
    except Exception as e:
        logger.error("Create failed: %s", e)
        return jsonify({"error": "Failed to create entity", "details": str(e)}), 500


@app.route("/telemetry", methods=["GET"])
def read_entities():
    tc = get_table_client()
    if tc is None:
        return jsonify({"error": "Storage not configured or SDK missing"}), 500
    try:
        device_id = request.args.get("deviceId")
        if device_id:
            filter_query = f"PartitionKey eq '{device_id}'"
            logger.info("Query filter: %s", filter_query)
            # azure.data.tables uses 'query_filter' kw name
            entities_iter = tc.query_entities(query_filter=filter_query)
        else:
            logger.info("Querying all entities")
            entities_iter = tc.list_entities()

        # Materialize to list (consider server-side paging later)
        entities = [e for e in entities_iter]
        return jsonify(entities), 200
    except Exception as e:
        logger.error("Read failed: %s", e)
        return jsonify({"error": "Failed to query data from Azure", "details": str(e)}), 500


@app.route("/telemetry/<row_key>", methods=["PUT"])
def update_entity(row_key):
    tc = get_table_client()
    if tc is None:
        return jsonify({"error": "Storage not configured or SDK missing"}), 500
    try:
        data = request.get_json(force=True) or {}
        partition_key = data.get("deviceId", "unknown")
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            **data
        }
        tc.update_entity(entity, mode="MERGE")
        logger.info("Updated entity %s for %s", row_key, partition_key)
        return jsonify({"status": "updated", "entity": entity}), 200
    except Exception as e:
        logger.error("Update failed: %s", e)
        return jsonify({"error": "Failed to update entity", "details": str(e)}), 500


@app.route("/telemetry/<row_key>", methods=["DELETE"])
def delete_entity(row_key):
    tc = get_table_client()
    if tc is None:
        return jsonify({"error": "Storage not configured or SDK missing"}), 500
    try:
        partition_key = request.args.get("deviceId", "unknown")
        tc.delete_entity(partition_key, row_key)
        logger.info("Deleted entity %s for %s", row_key, partition_key)
        return jsonify({"status": "deleted"}), 200
    except Exception as e:
        logger.error("Delete failed: %s", e)
        return jsonify({"error": "Failed to delete entity", "details": str(e)}), 500

# New app insights addition #
ai_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
if ai_conn:
    try:
        ai_handler = AzureLogHandler(connection_string=ai_conn)
        ai_handler.setLevel(logging.INFO)

        # Attach to your module logger
        logger.addHandler(ai_handler)
        # Attach to root logger so all logs flow to AI
        logging.getLogger().addHandler(ai_handler)

        logger.info("Application Insights logging enabled")
    except Exception as e:
        logger.warning("Failed to attach AI handler: %s", e)

# Local dev only; in Azure, Oryx runs gunicorn app:app

if __name__ == "__main__":
    app.run(debug=True)
