import base64
import json
import os
import logging
import time
import threading
from concurrent.futures import TimeoutError

# Import Google Cloud client libraries
from google.cloud import pubsub_v1
from google.cloud import bigquery
# We no longer need the low-level storage API clients for this robust approach
from google.api_core.exceptions import GoogleAPIError
from flask import Flask, Response

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Data Parsing Logic (assuming this is correct for your data) ---
def parse_visit(element: str):
    """Parses a JSON string representing a user visit."""
    from datetime import datetime
    try:
        visit_data = json.loads(element)
        session_id = visit_data.get("session_id")
        user_id = visit_data.get("user_id")
        device_type = visit_data.get("device_type")
        geo_str = visit_data.get("geolocation")
        if geo_str and ',' in geo_str:
            lat, lon = geo_str.split(',')
            geolocation = f"POINT({lon} {lat})"
        else:
            geolocation = None
        user_agent = visit_data.get("user_agent")
        events = visit_data.get("events", [])
        visit_start_time = None
        visit_end_time = None
        formatted_events = []
        for event_data in events:
            event = event_data.get("event", {})
            event_type = event.get("event_type")
            timestamp_str = event.get("timestamp")
            # Handle 'Z' for UTC timezone correctly
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            if visit_start_time is None or timestamp < visit_start_time:
                visit_start_time = timestamp
            if visit_end_time is None or timestamp > visit_end_time:
                visit_end_time = timestamp
            details = event.get("details", {})
            # Ensure nested structs are None if not applicable, to match BQ schema
            formatted_events.append({
                "event_type": event_type,
                "event_timestamp": timestamp.isoformat(),
                "page_view": details if event_type == "page_view" else None,
                "add_cart": details if event_type == "add_item_to_cart" else None,
                "purchase": details if event_type == "purchase" else None,
            })
        row = {
            "session_id": session_id,
            "user_id": user_id,
            "device_type": device_type,
            "geolocation": geolocation,
            "user_agent": user_agent,
            "visit_start_time": visit_start_time.isoformat() if visit_start_time else None,
            "visit_end_time": visit_end_time.isoformat() if visit_end_time else None,
            "events": formatted_events,
        }
        return row
    except Exception as e:
        logging.error(f"Error processing element in parse_visit: {e}", exc_info=True)
        logging.error(f"Problematic element for parse_visit: {element}")
        return None

# --- Configuration from Environment Variables ---
PROJECT_ID = "jellyfish-training-demo-6"
SUBSCRIPTION_ID = "dsl-project-clickstream-sub"
BIGQUERY_DATASET = "dsl_project"
BIGQUERY_TABLE = "web_visits"
FLASK_PORT = int(os.environ.get("FLASK_PORT", 8080))

# Fully qualified BigQuery table ID
FULL_BIGQUERY_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# --- BigQueryWriter Class using the recommended high-level client (FIXED) ---
class BigQueryWriter:
    """
    A simple, robust writer that uses the standard BigQuery client.
    It automatically leverages the Storage Write API for performance when available.
    """
    def __init__(self, table_id: str):
        # Initialize the standard BigQuery client.
        self.bq_client = bigquery.Client()
        self.table_id = table_id
        logging.info(f"Initialized BigQueryWriter for table: {table_id}")

    def insert_rows(self, rows: list):
        """Inserts a list of rows (as dictionaries) into the BigQuery table."""
        if not rows:
            return []

        try:
            # This is the recommended method for streaming inserts.
            # It's robust, handles serialization, and uses the Storage Write API under the hood.
            errors = self.bq_client.insert_rows_json(self.table_id, rows)
            if not errors:
                logging.info(f"Successfully inserted {len(rows)} row(s) into {self.table_id}")
                return []
            else:
                logging.error(f"Encountered errors while inserting rows: {errors}")
                return errors
        except Exception as e:
            logging.error(f"Unexpected error during insert_rows: {e}", exc_info=True)
            return [str(e)]

    def close(self):
        """Closes the underlying client."""
        if self.bq_client:
            self.bq_client.close()
            logging.info("BigQuery client closed.")


# --- Global Clients and State ---
bq_writer = None
subscriber_client = None
pull_thread = None
pull_thread_stop_event = threading.Event()

# --- Pub/Sub Message Processing (Simplified) ---
def process_payload(message: pubsub_v1.subscriber.message.Message):
    """Processes a single Pub/Sub message."""
    if not bq_writer:
        logging.critical("bq_writer is None. Nacking message.")
        message.nack()
        return

    try:
        data_str = message.data.decode("utf-8").strip()
        if not data_str:
            logging.warning(f"Acking empty message: {message.message_id}")
            message.ack()
            return

        parsed_data = parse_visit(data_str)
        if parsed_data:
            # The insert_rows method is now blocking and returns errors directly.
            errors = bq_writer.insert_rows([parsed_data])
            if not errors:
                logging.info(f"Write successful for message {message.message_id}. Acking.")
                message.ack()
            else:
                logging.error(f"Write to BigQuery failed for message {message.message_id}: {errors}. Nacking.")
                message.nack()
        else:
            logging.error(f"Failed to parse message data. Acking to avoid poison pill: {message.message_id}")
            message.ack()

    except Exception as e:
        logging.error(f"Unhandled error processing message {message.message_id}: {e}", exc_info=True)
        message.nack()


# --- PULL Subscriber Thread Logic ---
def run_pull_subscriber():
    """Target function for the subscriber thread."""
    global subscriber_client
    subscription_path = subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    # Using a scheduler can improve performance for high-throughput scenarios.
    scheduler = pubsub_v1.subscriber.scheduler.ThreadScheduler()
    flow_control = pubsub_v1.types.FlowControl(max_messages=50)

    streaming_pull_future = subscriber_client.subscribe(
        subscription_path,
        callback=process_payload,
        flow_control=flow_control,
        scheduler=scheduler
    )
    logging.info(f"Pub/Sub subscriber started on {subscription_path}. Listening for messages.")

    # Keep the thread alive until the main thread signals it to stop.
    pull_thread_stop_event.wait()

    # Gracefully shut down the subscriber.
    streaming_pull_future.cancel()
    try:
        streaming_pull_future.result(timeout=30)
    except TimeoutError:
        logging.warning("Timeout waiting for subscriber to shut down.")
    logging.info("Pub/Sub subscriber has stopped.")


# --- Flask App for Health Checks ---
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Provides a health check endpoint for cloud infrastructure."""
    if pull_thread and pull_thread.is_alive():
        return Response("Pull subscriber is healthy.", status=200)
    else:
        return Response("Pull subscriber is unhealthy or has stopped.", status=503)

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Application starting up...")

    try:
        # Initialize the robust BigQuery writer first.
        logging.info("Initializing BigQuery Writer...")
        bq_writer = BigQueryWriter(FULL_BIGQUERY_TABLE_ID)
        logging.info("BigQuery Writer initialized.")

        logging.info("Initializing Pub/Sub client...")
        subscriber_client = pubsub_v1.SubscriberClient()
        logging.info("Pub/Sub client initialized.")

        logging.info("Starting Pub/Sub subscriber thread...")
        pull_thread = threading.Thread(target=run_pull_subscriber, daemon=True)
        pull_thread.start()

        logging.info(f"Starting Flask health check server on port {FLASK_PORT}.")
        app.run(host='0.0.0.0', port=FLASK_PORT, use_reloader=False)

    except Exception as e:
        logging.critical(f"A critical error occurred during startup: {e}", exc_info=True)
    finally:
        logging.info("Shutdown signal received. Cleaning up resources...")

        # Signal the subscriber thread to stop and wait for it.
        pull_thread_stop_event.set()
        if pull_thread and pull_thread.is_alive():
             pull_thread.join(timeout=15)

        # Close clients.
        if bq_writer:
            bq_writer.close()
        if subscriber_client:
            subscriber_client.close()

        logging.info("Application shut down gracefully.")
