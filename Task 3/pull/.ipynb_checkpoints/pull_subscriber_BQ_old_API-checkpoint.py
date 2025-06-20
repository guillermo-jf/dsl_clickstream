import base64
import json
import os
import logging
import time
import threading
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from google.cloud import bigquery
from flask import Flask, Response

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- Data Parsing Logic ---
def parse_visit(element: str):
    import json
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
            geolocation = None # Or handle error appropriately
        user_agent = visit_data.get("user_agent")
        events = visit_data.get("events", [])
        visit_start_time = None
        visit_end_time = None
        formatted_events = []
        for event_data in events:
            event = event_data.get("event", {})
            event_type = event.get("event_type")
            timestamp_str = event.get("timestamp")
            timestamp = datetime.fromisoformat(timestamp_str)
            if visit_start_time is None or timestamp < visit_start_time:
                visit_start_time = timestamp
            if visit_end_time is None or timestamp > visit_end_time:
                visit_end_time = timestamp
            details = event.get("details", {})
            page_view_details = {}
            add_cart_details = {}
            purchase_details = {}
            if event_type == "page_view":
                page_view_details = {
                    "page_url": details.get("page_url"),
                    "referrer_url": details.get("referrer_url"),
                }
            elif event_type == "add_item_to_cart":
                add_cart_details = {
                    "product_id": details.get("product_id"),
                    "product_name": details.get("product_name"),
                    "category": details.get("category"),
                    "price": details.get("price"),
                    "quantity": details.get("quantity"),
                }
            elif event_type == "purchase":
                purchase_details = {
                    "order_id": details.get("order_id"),
                    "amount": details.get("amount"),
                    "currency": details.get("currency"),
                    "items": details.get("items"),
                }
            formatted_events.append({
                "event_type": event_type,
                "event_timestamp": timestamp.isoformat(),
                "page_view": page_view_details,
                "add_cart": add_cart_details,
                "purchase": purchase_details,
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
PROJECT_ID = "jellyfish-training-demo-6" #os.environ.get("GCP_PROJECT")
SUBSCRIPTION_ID = "dsl-project-clickstream-sub" #os.environ.get("PUBSUB_SUBSCRIPTION_ID") # e.g., "my-subscription-for-visits"
BIGQUERY_DATASET = "dsl_project" #os.environ.get("BIGQUERY_DATASET", "visit_data")
BIGQUERY_TABLE = "web_visits" #os.environ.get("BIGQUERY_TABLE", "visits")
FLASK_PORT = 8080 #int(os.environ.get("FLASK_PORT", 8080))

# --- Global Clients and State (Initialize once) ---
bq_client = None
subscriber_client = None # Will be initialized in main
subscription_path = None # Will be set in main

streaming_pull_future = None  # For PULL mode health check and management
pull_subscriber_healthy = False # Flag to indicate PULL subscriber thread health
pull_thread_active = threading.Event() # To signal the pull thread to stop

# Initialize BigQuery Client
if PROJECT_ID:
    bq_client = bigquery.Client(project=PROJECT_ID)
else:
    logging.critical("GCP_PROJECT environment variable not set. BigQuery client cannot be initialized. Exiting.")
    exit(1)

if not SUBSCRIPTION_ID:
    logging.critical("PUBSUB_SUBSCRIPTION_ID environment variable not set. Exiting.")
    exit(1)

if not BIGQUERY_DATASET or not BIGQUERY_TABLE:
    logging.critical("BIGQUERY_DATASET or BIGQUERY_TABLE environment variables not set. Exiting.")
    exit(1)


# --- Pub/Sub Message Processing ---
def process_payload(message: pubsub_v1.subscriber.message.Message):
    """
    Processes a single Pub/Sub message.
    """
    global bq_client # Ensure we're using the global client

    data_str = ""
    try:
        data_str = message.data.decode("utf-8").strip()
        logging.info(f"Received message: {message.message_id}")#, data snippet: {data_str[:100]}...")
    except Exception as e:
        logging.error(f"Error decoding message data for message {message.message_id}: {e}", exc_info=True)
        #TODO: Send to a GCS bucket for human review
        # Acknowledge messages that cannot be decoded to prevent reprocessing loops
        message.ack()
        return

    if not data_str:
        logging.warning(f"Received empty data for message {message.message_id} after decoding.")
        message.ack()
        return

    parsed_data = parse_visit(data_str)

    if parsed_data:
        #logging.debug(f"Parsed data for message {message.message_id}: {parsed_data}")
        if bq_client:
            try:
                table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
                errors = bq_client.insert_rows_json(table_id, [parsed_data])
                if not errors:
                    logging.info(f"Successfully inserted row into {table_id} for message {message.message_id}")
                    message.ack() # Acknowledge successful processing
                else:
                    logging.error(f"Errors encountered while inserting rows into {table_id} for message {message.message_id}: {errors}")
                    # Do not acknowledge, let Pub/Sub redeliver for retry (up to configured limits)
                    message.nack()
            except Exception as e:
                logging.error(f"Error inserting data into BigQuery for message {message.message_id}: {e}", exc_info=True)
                # Do not acknowledge, let Pub/Sub redeliver for retry
                message.nack()
        else:
            # This case should ideally not be reached due to the check at startup
            logging.critical("BigQuery client not initialized (should have exited at startup). Cannot insert data. Nacking message.")
            message.nack()
    else:
        logging.warning(f"Failed to parse message data for message {message.message_id}. Original data: {data_str}")
        # Acknowledge messages that fail parsing to prevent reprocessing loops of bad data
        message.ack()

# --- PULL Subscriber Thread Logic ---
def run_pull_subscriber_thread():
    """
    Starts the PULL subscriber in the current thread.
    Manages the lifecycle of the streaming pull and updates health status.
    """
    global streaming_pull_future, pull_subscriber_healthy, subscriber_client, subscription_path, pull_thread_active

    if not subscriber_client or not subscription_path:
        logging.critical("PULL Subscriber Thread: Client or subscription path not initialized. Thread will not start.")
        pull_subscriber_healthy = False
        return

    current_future = None
    try:
        flow_control = pubsub_v1.types.FlowControl(max_messages=10) # Adjust as needed
        logging.info(f"PULL Subscriber Thread: Attempting to subscribe to {subscription_path}...")
        current_future = subscriber_client.subscribe(
            subscription_path,
            callback=process_payload,
            flow_control=flow_control
        )
        streaming_pull_future = current_future # Make it globally visible

        logging.info(f"PULL Subscriber Thread: Successfully subscribed. Listening for messages on {subscription_path}.")
        pull_subscriber_healthy = True # Mark as healthy

        # Keep the thread alive and check for shutdown signal
        while not pull_thread_active.is_set() and current_future.running():
            try:
                current_future.result(timeout=1) # Check future status periodically
            except TimeoutError:
                continue # Normal, means it's still running
            except Exception as e: # Future completed with an error
                logging.error(f"PULL Subscriber Thread: Pub/Sub future completed with error: {e}", exc_info=True)
                pull_subscriber_healthy = False
                break # Exit loop on future error

        if pull_thread_active.is_set():
            logging.info("PULL Subscriber Thread: Shutdown signal received.")

    except Exception as e:
        logging.error(f"PULL Subscriber Thread: Encountered an unhandled error during setup or execution: {e}", exc_info=True)
        pull_subscriber_healthy = False
    finally:
        logging.info("PULL Subscriber Thread: Shutting down...")
        if current_future:
            if not current_future.cancelled() and current_future.running():
                logging.info("PULL Subscriber Thread: Cancelling active Pub/Sub future.")
                current_future.cancel()
            try:
                current_future.result(timeout=10) # Wait for cancellation to complete
                logging.info("PULL Subscriber Thread: Pub/Sub future successfully cancelled/completed.")
            except TimeoutError:
                logging.warning("PULL Subscriber Thread: Timeout waiting for Pub/Sub future to cancel.")
            except Exception as e:
                logging.error(f"PULL Subscriber Thread: Error during future result after cancel: {e}", exc_info=True)

        pull_subscriber_healthy = False
        streaming_pull_future = None # Clear the global future
        logging.info("PULL Subscriber Thread: Finished.")

# --- Flask App for Health Checks ---
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Provides a health check endpoint for MIGs."""
    global pull_subscriber_healthy, streaming_pull_future, subscriber_client

    if subscriber_client and streaming_pull_future and streaming_pull_future.running() and pull_subscriber_healthy:
        return Response("Pull subscriber is healthy.", status=200)
    else:
        reasons = []
        if not subscriber_client: reasons.append("Subscriber client not initialized.")
        if not streaming_pull_future: reasons.append("Streaming pull future not started.")
        elif not streaming_pull_future.running(): reasons.append(f"Streaming pull future not running (Done: {streaming_pull_future.done()}, Cancelled: {streaming_pull_future.cancelled()}).")
        if not pull_subscriber_healthy: reasons.append("Pull subscriber thread reported unhealthy or has exited.")
        logging.warning(f"Health Check: Unhealthy. Reasons: {'; '.join(reasons)}")
        return Response(f"Pull subscriber unhealthy: {'; '.join(reasons)}", status=503)

if __name__ == "__main__":
    logging.info("Application starting...")

    # Initialize Pub/Sub client and path globally for access by health check and thread
    try:
        subscriber_client = pubsub_v1.SubscriberClient()
        subscription_path = subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
        logging.info(f"Pub/Sub client initialized for subscription: {subscription_path}")
    except Exception as e:
        logging.critical(f"Failed to initialize Pub/Sub client: {e}. Exiting.", exc_info=True)
        exit(1)

    # Start the PULL subscriber in a background thread
    pull_thread = threading.Thread(target=run_pull_subscriber_thread, daemon=True)
    pull_thread.start()
    logging.info("PULL subscriber thread initiated.")

    # Start the Flask app for health checks
    logging.info(f"Starting HTTP server for health checks on port {FLASK_PORT} at /health")
    try:
        # Use a production-ready WSGI server (e.g., gunicorn) in production
        # For simplicity here, we use Flask's built-in server.
        app.run(host='0.0.0.0', port=FLASK_PORT, use_reloader=False)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Shutting down...")
    finally:
        logging.info("Flask server stopping. Signaling PULL subscriber thread to stop...")
        pull_thread_active.set() # Signal the thread to stop
        if pull_thread.is_alive():
            pull_thread.join(timeout=15) # Wait for the thread to finish
        if pull_thread.is_alive():
            logging.warning("PULL subscriber thread did not stop in time.")

        if subscriber_client:
            subscriber_client.close()
            logging.info("Pub/Sub subscriber client closed.")
        logging.info("Application shut down gracefully.")
