# main.py
import base64
import json
import os
from datetime import datetime
from flask import Flask, request
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

PROJECT_ID = os.environ.get("PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "dsl_project")
BIGQUERY_TABLE = os.environ.get("BIGQUERY_TABLE", "web_visits")
TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

def transform_event(event_item):
    """Transforms a single event from the source JSON to the BigQuery schema."""
    event_data = event_item.get("event", {})
    event_type = event_data.get("event_type")
    details = event_data.get("details", {})

    transformed = {
        "event_type": event_type,
        "event_timestamp": event_data.get("timestamp"),
        "page_view": None,
        "add_cart": None,
        "purchase": None,
    }

    if event_type == "page_view":
        transformed["page_view"] = {
            "page_url": details.get("page_url"),
            "referrer_url": details.get("referrer_url"),
        }
    elif event_type == "add_item_to_cart":
        transformed["add_cart"] = {
            "product_id": details.get("product_id"),
            "product_name": details.get("product_name"),
            "category": details.get("category"),
            "price": details.get("price"),
            "quantity": details.get("quantity"),
        }
    # Add logic for 'purchase' event type if it exists in your data
    # elif event_type == "purchase":
    #     transformed["purchase"] = { ... }

    return transformed

@app.route("/", methods=["POST"])
def index():
    """Receives and processes a push message from a Pub/Sub subscription."""
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    message = envelope["message"]
    rows_to_insert = []

    if "data" in message:
        data_str = base64.b64decode(message["data"]).decode("utf-8").strip()
        for line in data_str.splitlines():
            try:
                data = json.loads(line)
                events = data.get("events", [])
                
                if not events:
                    continue

                # Calculate visit start and end times
                timestamps = [e["event"]["timestamp"] for e in events if "timestamp" in e.get("event", {})]
                date_timestamps = [datetime.fromisoformat(ts) for ts in timestamps]
                
                # Transform events to match BQ schema
                transformed_events = [transform_event(e) for e in events]

                row = {
                    "session_id": data.get("session_id"),
                    "user_id": data.get("user_id"),
                    "device_type": data.get("device_type"),
                    "geolocation": data.get("geolocation"),
                    "user_agent": data.get("user_agent"),
                    "visit_start_time": min(date_timestamps).isoformat() if date_timestamps else None,
                    "visit_end_time": max(date_timestamps).isoformat() if date_timestamps else None,
                    "events": transformed_events,
                }
                rows_to_insert.append(row)

            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error processing line: {e} - Line: '{line}'")
                continue

    if not rows_to_insert:
        print("No rows to insert.")
        return "Success: No data to process", 200

    errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
    if not errors:
        print(f"Successfully inserted {len(rows_to_insert)} rows into {TABLE_ID}")
        return "Success", 204
    else:
        print(f"Encountered errors while inserting rows: {errors}")
        return f"Error inserting data: {errors}", 500

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=PORT, debug=True)