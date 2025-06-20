import base64
import json
import os
from google.cloud import bigquery

# Configure BigQuery client
PROJECT_ID = os.environ.get("GCP_PROJECT")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "visit_data")
BIGQUERY_TABLE = os.environ.get("BIGQUERY_TABLE", "visits")

# Initialize BigQuery client only if project_id is available
# This helps in local testing where GCP_PROJECT might not be set
if PROJECT_ID:
    client = bigquery.Client(project=PROJECT_ID)
else:
    client = None
    print("Warning: GCP_PROJECT environment variable not set. BigQuery client not initialized.")

# Function to parse events into BQ rows.
def parse_visit(element: str):
    """
    Parses a JSON string representing a user visit and extracts relevant information.

    Args:
        element (str): A JSON string containing visit data.
    Returns:
        dict: A dictionary containing parsed visit data, or None if an error occurs.
    """
    
    import json
    from datetime import datetime

    try:
        visit_data = json.loads(element)

        session_id = visit_data.get("session_id")
        user_id = visit_data.get("user_id")
        device_type = visit_data.get("device_type")
        
        #Parse geolocation data
        geo_str = visit_data.get("geolocation")
        lat, lon = geo_str.split(',')
        geolocation = f"POINT({lon} {lat})"  # Convert to WKT

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

            formatted_events.append(
                {
                    "event_type": event_type,
                    "event_timestamp": timestamp.isoformat(),
                    "page_view": page_view_details,
                    "add_cart": add_cart_details,
                    "purchase": purchase_details,
                }
            )

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
        
        return (row)
    except Exception as e:
        print(f"Error processing element: {e}")
        print(f"Problematic element: {element}")
        return None


def process_pubsub_message(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `data` field contains the Pub/Sub message
                        data, base64-encoded.
         context (google.cloud.functions.Context): Metadata of triggering event.
    """
    data = ""
    if 'data' in event:
        try:
            data = base64.b64decode(event['data']).decode('utf-8').strip()
        except Exception as e:
            print(f"Error decoding Pub/Sub message data: {e}")
            # Non-retryable error, so acknowledge the message by returning
            return
    else:
        print("Warning: Pub/Sub message data not found in event.")
        # Acknowledge the message
        return

    if not data:
        print("Received empty data after decoding.")
        # Decide if this is an error or just an empty message to acknowledge
        return

    #print(f"Received message: {data}")
    #print(f"Event ID: {context.event_id}")
    #print(f"Timestamp: {context.timestamp}")

    # Process the message using the parse_visit function
    parsed_data = parse_visit(data)

    if parsed_data:
        print(f"Parsed data: {parsed_data}")
        if client:
            try:
                table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
                errors = client.insert_rows_json(table_id, [parsed_data])
                if errors:
                    print(f"Errors encountered while inserting rows into {table_id}: {errors}")
                    # Raise an exception to signal Pub/Sub to retry (if configured)
                    raise Exception(f"BigQuery insertion errors: {errors}")
            except Exception as e:
                print(f"Error inserting data into BigQuery: {e}")
                # Raise an exception to signal Pub/Sub to retry
                raise
        else:
            print("BigQuery client not initialized. Skipping BigQuery insert.")
            raise Exception("BigQuery client not initialized.")
    else:
        print(f"Failed to parse message: {data}")
        # Message could not be parsed, acknowledge to Pub/Sub to prevent retries for bad messages
        # Returning None (or simply returning) acknowledges the message.
        return