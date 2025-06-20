import json
import random
import uuid
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import os # For potential future use with environment variables
import argparse
import time
import logging

# Placeholder for products data
PRODUCTS = []

def load_products(filename="products.json"):
    global PRODUCTS
    # In a real scenario, this file path would need to be relative to this script,
    # or an absolute path. For now, assume it's in the same directory or adjust as needed.
    # The worker should make sure this path is correct based on the project structure.
    # Correct path should be 'Task3/products.json' relative to repo root.
    try:
        with open(filename, 'r') as f:
            PRODUCTS = json.load(f)
    except FileNotFoundError:
        logging.error(f"Product file {filename} not found.")
        # Potentially re-raise or handle as a critical error
        PRODUCTS = [] # Ensure PRODUCTS is empty if file not found
    return PRODUCTS

def generate_session_id():
    """Returns a unique string (e.g., UUID)."""
    return str(uuid.uuid4())

def generate_user_id():
    """Returns a unique string (e.g., UUID)."""
    return str(uuid.uuid4())

def generate_device_type():
    """Returns one of "desktop", "mobile", "tablet"."""
    return random.choice(["desktop", "mobile", "tablet"])

def generate_geolocation():
    """Returns a string like "POINT(lon lat)" with random coordinates."""
    lon = random.uniform(-180, 180)
    lat = random.uniform(-90, 90)
    return f"POINT({lon:.6f} {lat:.6f})"

def generate_user_agent():
    """Returns a common user agent string."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 16_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/108.0.5359.112 Mobile/15E148 Safari/604.1",
    ]
    return random.choice(user_agents)

def generate_event(current_time, products):
    """Generates a single event object."""
    if not products: # Ensure products are loaded
        print("Cannot generate event: Products not loaded.")
        return None

    event_type = random.choices(
        ["page_view", "add_cart", "purchase"], 
        weights=[0.6, 0.3, 0.1], 
        k=1
    )[0]

    # Ensure event_timestamp is slightly later than current_time
    event_timestamp = current_time + timedelta(seconds=random.randint(1, 60))
    
    event = {
        "event_type": event_type,
        "event_timestamp": event_timestamp.isoformat() + "Z", # ISO 8601 format
    }

    if event_type == "page_view":
        page_types = ["product_detail", "category", "homepage", "search_results", "checkout_page"]
        page_type = random.choice(page_types)
        
        if page_type == "product_detail" and products:
            product = random.choice(products)
            page_url = f"https://example.com/products/{product['product_id']}/{product['product_name'].lower().replace(' ', '-')}"
        elif page_type == "category" and products:
            product = random.choice(products) # Pick a product to get a category
            page_url = f"https://example.com/category/{product['category'].lower().replace(' ', '-')}"
        elif page_type == "search_results":
            search_terms = ["laptop", "phone", "book", "kitchenware", "shoes"]
            page_url = f"https://example.com/search?q={random.choice(search_terms)}"
        elif page_type == "checkout_page":
            page_url = "https://example.com/checkout"
        else: # homepage or fallback
            page_url = "https://example.com/"

        referrers = [
            "https://google.com", 
            "https://bing.com", 
            "https://facebook.com", 
            "https://example.com/other-page", # internal referrer
            None # No referrer
        ]
        referrer_url = random.choice(referrers)
        
        event["page_view"] = {
            "page_url": page_url,
            "referrer_url": referrer_url
        }
    elif event_type == "add_cart":
        product = random.choice(products)
        quantity = random.randint(1, 3)
        event["add_cart"] = {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "price": float(product["price"]),
            "quantity": quantity
        }
    elif event_type == "purchase":
        num_items_in_purchase = random.randint(1, 3)
        items_purchased = []
        total_amount = 0.0

        for _ in range(num_items_in_purchase):
            product = random.choice(products)
            quantity = random.randint(1, 2)
            item_total_price = float(product["price"]) * quantity
            
            items_purchased.append({
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "price": float(product["price"]),
                "quantity": quantity
            })
            total_amount += item_total_price
        
        event["purchase"] = {
            "order_id": str(uuid.uuid4()), # Unique order ID
            "amount": round(total_amount, 2),
            "currency": "USD",
            "items": items_purchased
        }
    
    return event

def generate_visit(products):
    """Generates a single visit object with multiple events."""
    if not products:
        print("Cannot generate visit: Products not loaded.")
        return None

    session_id = generate_session_id()
    user_id = generate_user_id() # Could be the same for multiple visits from the same user
    device_type = generate_device_type()
    geolocation = generate_geolocation()
    user_agent = generate_user_agent()

    events = []
    num_events = random.randint(1, 5) # Generate 1 to 5 events per visit

    # Initial time for the first event in the session
    # Simulate visits occurring recently, e.g., within the last 7 days
    session_start_time = datetime.now() - timedelta(days=random.randint(0, 7), 
                                                    hours=random.randint(0, 23), 
                                                    minutes=random.randint(0, 59))
    current_event_time = session_start_time

    for _ in range(num_events):
        event = generate_event(current_event_time, products)
        if event:
            events.append(event)
            # Update current_event_time for the next event based on the timestamp of the one just generated
            # Parse the ISO format string back to datetime object
            current_event_time = datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
        else:
            # Handle case where event generation might fail (e.g., if products list was empty during a call)
            print(f"Warning: Failed to generate an event for session {session_id}")


    if not events: # If no events were generated, perhaps return None or an empty visit
        print(f"Warning: No events generated for session {session_id}. Returning None for visit.")
        return None

    # visit_start_time is the timestamp of the first event
    # visit_end_time is the timestamp of the last event
    visit_start_time_iso = events[0]["event_timestamp"]
    visit_end_time_iso = events[-1]["event_timestamp"]

    visit_data = {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": device_type,
        "geolocation": geolocation,
        "user_agent": user_agent,
        "visit_start_time": visit_start_time_iso,
        "visit_end_time": visit_end_time_iso,
        "events": events
    }
    
    return visit_data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load products
    # The worker should adjust the path to "Task3/products.json"
    parser = argparse.ArgumentParser(description="Visit data simulator for Pub/Sub.")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID.")
    parser.add_argument("--topic_id", required=True, help="Pub/Sub Topic ID.")
    parser.add_argument("--visits_per_minute", type=int, default=60, help="Number of visits to generate per minute.")
    parser.add_argument("--duration_minutes", type=int, default=1, help="Total duration for the simulator to run in minutes.")
    args = parser.parse_args()

    products_data = load_products("Task 3/products.json") 

    if not products_data:
        logging.error("No product data loaded. Exiting simulator.")
        exit() # Exit if no products

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic_id)

    delay_seconds = 60.0 / args.visits_per_minute
    start_time = time.time() 
    end_time = start_time + args.duration_minutes * 60
    
    logging.info(f"Starting simulator for {args.duration_minutes} minute(s), generating {args.visits_per_minute} visits per minute...")
    logging.info(f"Publishing to topic: {topic_path}")

    try:
        while time.time() < end_time:
            visit_to_publish = generate_visit(products_data)
            if not visit_to_publish: # generate_visit can return None
                # Consider changing this to logging.warning or logging.info if it's not critical
                logging.warning("Skipping iteration due to failed visit generation.") 
                continue

            visit_json_data = json.dumps(visit_to_publish)
            visit_bytes_data = visit_json_data.encode("utf-8")
            
            # publish event to topic
            future = publisher.publish(topic_path, data=visit_bytes_data)
            # Consider adding a callback for more robust error handling if needed
            logging.info(f"Published message ID: {future.result(timeout=10)} to topic {topic_path}")

            time.sleep(delay_seconds)
            
    except Exception as e:
        logging.error(f"An error occurred during simulation: {e}")
    finally:
        logging.info(f"Simulator finished after {args.duration_minutes} minute(s).")
