"""
This module provides functions for parsing and processing visit data.
"""

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