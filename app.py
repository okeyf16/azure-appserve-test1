import os
import uuid
import logging
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient, TableServiceError
from dotenv import load_dotenv

# Optional: App Insights via opencensus (only enable if env var present)
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    OPENCENSUS_AVAILABLE = True
except Exception:
    OPENCENSUS_AVAILABLE = False

# Load local .env for dev only
load_dotenv()

# App and logger
app = Flask(__name__)
logger = logging.getLogger("telemetry_api")
logger.setLevel(logging.INFO)
# Avoid duplicate handlers in some hosting scenarios
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)

# App Insights handler (safe: only add when valid conn string present and opencensus installed)
ai_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")  # set this in App Service config
if ai_conn and OPENCENSUS_AVAILABLE:
    # Keep the connection string minimal: InstrumentationKey=...;IngestionEndpoint=...
    try:
        ai_handler = AzureLogHandler(connection_string=ai_conn)
        ai_handler.setLevel(logging.INFO)
        logger.addHandler(ai_handler)
        logger.info("Application Insights logging enabled")
    except Exception as ex:
        logger.warning("Failed to add AzureLogHandler: %s", ex)

# Required environment variables check
API_KEY = os.getenv("API_KEY")
TABLE_NAME = os.getenv("TABLE_NAME", "TelemetryData")
STORAGE_CONN_STR = os.getenv("STORAGE_CONN_STR")

if not STORAGE_CONN_STR:
    logger.error("STORAGE_CONN_STR not set. App cannot access Table Storage.")
# Initialize table client safely
table_client = None
if STORAGE_CONN_STR:
    try:
        svc = TableServiceClient.from_connection_string(STORAGE_CONN_STR)
        table_client = svc.get_table_client(table_name=TABLE_NAME)
        logger.info("Connected to Table Storage table: %s", TABLE_NAME)
    except Exception as e:
        logger.exception("Failed to create TableServiceClient: %s", e)
        table_client = None

# Health endpoint for Azure to probe
@app.route("/healthz", methods=["GET"])
def healthz():
    # Basic checks: table client and API key presence
    ok = True
    details = {}
    details["table_client"] = bool(table_client)
    details["api_key_set"] = bool(API_KEY)
    status = 200 if all(details.values()) else 500
    return jsonify({"ok": all(details.values()), "details": details}), status

# API key middleware
@app.before_request
def check_api_key():
    # Allow health check without API key
    if request.path == "/healthz":
        return None
    key = request.headers.get("x-api-key")
    if not API_KEY or key != API_KEY:
        logger.warning("Unauthorized request to %s from %s", request.path, request.remote_addr)
        return jsonify({"error": "Unauthorized"}), 401
    return None

# Create new entity
@app.route("/telemetry", methods=["POST"])
def create_entity():
    if table_client is None:
        return jsonify({"error": "Storage not configured"}), 500
    try:
        data = request.get_json(force=True)
        partition = data.get("deviceId", "unknown")
        entity = {
            "PartitionKey": partition,
            "RowKey": str(uuid.uuid4()),
            **data
        }
        table_client.create_entity(entity)
        logger.info("Created entity for device %s row %s", partition, entity["RowKey"])
        return jsonify({"status": "created", "entity": entity}), 201
    except Exception as e:
        logger.exception("create_entity failed: %s", e)
        return jsonify({"error": "Failed to create entity", "details": str(e)}), 500

# Read entities (latest or last N)
@app.route("/telemetry", methods=["GET"])
def read_entities():
    if table_client is None:
        return jsonify({"error": "Storage not configured"}), 500
    try:
        device_id = request.args.get("deviceId")
        top = request.args.get("top", None)  # optional param to request last N entries
        # Default behavior: return all entities (but client should request top)
        if device_id:
            filter_query = f"PartitionKey eq '{device_id}'"
            logger.info("Querying telemetry for device: %s", device_id)
            entities_iter = table_client.query_entities(query_filter=filter_query)
        else:
            logger.info("Querying all telemetry entities")
            entities_iter = table_client.list_entities()

        # Collect entities into list (be cautious with large tables)
        entities = [e for e in entities_iter]

        # If timestamps exist, attempt to sort by timestamp-like keys
        # Prefer a known timestamp field name (e.g., 'timestamp', 'createdAt') if present
        ts_field = None
        for candidate in ("timestamp", "createdAt", "TimeGenerated", "time"):
            if any(candidate in e for e in entities):
                ts_field = candidate
                break

        if ts_field:
            entities.sort(key=lambda x: x.get(ts_field) or "", reverse=True)
        # If request asked for top (last N), trim list
        if top:
            try:
                n = int(top)
                entities = entities[:n]
            except ValueError:
                pass

        return jsonify(entities), 200
    except TableServiceError as te:
        logger.exception("Azure Table service error: %s", te)
        return jsonify({"error": "Storage error", "details": str(te)}), 500
    except Exception as e:
        logger.exception("read_entities failed: %s", e)
        return jsonify({"error": "Failed to query data", "details": str(e)}), 500

# Update entity
@app.route("/telemetry/<row_key>", methods=["PUT"])
def update_entity(row_key):
    if table_client is None:
        return jsonify({"error": "Storage not configured"}), 500
    try:
        data = request.get_json(force=True)
        partition_key = data.get("deviceId", "unknown")
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            **data
        }
        table_client.update_entity(entity, mode="MERGE")
        logger.info("Updated entity %s for %s", row_key, partition_key)
        return jsonify({"status": "updated", "entity": entity}), 200
    except Exception as e:
        logger.exception("update_entity failed: %s", e)
        return jsonify({"error": "Failed to update entity", "details": str(e)}), 500

# Delete entity
@app.route("/telemetry/<row_key>", methods=["DELETE"])
def delete_entity(row_key):
    if table_client is None:
        return jsonify({"error": "Storage not configured"}), 500
    try:
        partition_key = request.args.get("deviceId", "unknown")
        table_client.delete_entity(partition_key, row_key)
        logger.info("Deleted entity %s for %s", row_key, partition_key)
        return jsonify({"status": "deleted"}), 200
    except Exception as e:
        logger.exception("delete_entity failed: %s", e)
        return jsonify({"error": "Failed to delete entity", "details": str(e)}), 500

# Entrypoint: bind to 0.0.0.0:8000 which Azure expects
if __name__ == "__main__":
    logger.info("Starting Flask app on 0.0.0.0:8000")
    # For dev use only; in App Service the container will run this script too
    app.run(host="0.0.0.0", port=8000)
