import unittest
import json
from datetime import datetime, timezone
import os
import sys
import re # For geolocation regex

# Adjust sys.path to allow importing Task3.simulator
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) 
sys.path.insert(0, project_root)

from Task3 import simulator

class TestVisitGeneration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_dir = os.path.dirname(os.path.abspath(__file__)) # This is Task3 directory
        project_root_dir = os.path.dirname(base_dir) # This should be the repo root

        products_path = os.path.join(project_root_dir, "Task3", "products.json")
        schema_path = os.path.join(project_root_dir, "dsllib", "table_schema.json")

        cls.products = simulator.load_products(products_path) # Matched prompt variable name
        if not cls.products:
            if not os.path.exists(products_path):
                 raise unittest.SkipTest(f"Products file not found at {products_path}, skipping tests.")
            raise unittest.SkipTest(f"Products file {products_path} is empty, skipping tests.")

        # For efficient lookup in tests
        cls.products_map = {p['product_id']: p for p in cls.products}

        try:
            with open(schema_path, 'r') as f:
                cls.schema = json.load(f)
        except FileNotFoundError:
            raise unittest.SkipTest(f"Schema file not found at {schema_path}, skipping tests.")
        
        cls.schema_fields = {field['name']: field for field in cls.schema['fields']}
        
        # Pre-parse event schema fields for easier lookup in tests
        cls.event_schema_fields = {}
        events_top_field = next((f for f in cls.schema['fields'] if f['name'] == 'events'), None)
        if events_top_field and events_top_field['type'] == 'RECORD':
            for event_field_container in events_top_field['fields']: # event_type, event_timestamp, page_view, ...
                # Store schema for page_view, add_cart, purchase records themselves
                if event_field_container['type'] == 'RECORD' and 'fields' in event_field_container:
                    cls.event_schema_fields[event_field_container['name']] = \
                        {f['name']: f for f in event_field_container['fields']}
                    
                    # Specifically for items within purchase
                    if event_field_container['name'] == 'purchase':
                        items_sub_field = next((f for f in event_field_container['fields'] if f['name'] == 'items'), None)
                        if items_sub_field and items_sub_field['type'] == 'RECORD' and items_sub_field['mode'] == 'REPEATED':
                             cls.event_schema_fields['purchase_item'] = \
                                 {f['name']: f for f in items_sub_field['fields']}

    def setUp(self):
        # Generate a new visit for each test method
        self.visit_data = simulator.generate_visit(self.products) # Use cls.products
        self.assertIsNotNone(self.visit_data, "generate_visit should return data, not None, check product loading and generation logic.")
        self.assertTrue(len(self.visit_data["events"]) > 0, "A visit should have at least one event.")


    def test_top_level_fields_exist_and_types(self):
        self.assertIn("session_id", self.visit_data)
        self.assertIsInstance(self.visit_data["session_id"], str)

        self.assertIn("user_id", self.visit_data)
        self.assertIsInstance(self.visit_data["user_id"], str)

        self.assertIn("device_type", self.visit_data)
        self.assertIsInstance(self.visit_data["device_type"], str)
        self.assertIn(self.visit_data["device_type"], ["desktop", "mobile", "tablet"])

        self.assertIn("geolocation", self.visit_data)
        self.assertIsInstance(self.visit_data["geolocation"], str)
        # POINT(lon lat) with 6 decimal places for lon and lat
        self.assertRegex(self.visit_data["geolocation"], r"POINT\(-?\d{1,3}\.\d{6} -?\d{1,2}\.\d{6}\)")


        self.assertIn("user_agent", self.visit_data)
        self.assertIsInstance(self.visit_data["user_agent"], str)

        self.assertIn("visit_start_time", self.visit_data)
        self.assertIsInstance(self.visit_data["visit_start_time"], str)
        datetime.fromisoformat(self.visit_data["visit_start_time"].replace("Z", "+00:00")) 

        self.assertIn("visit_end_time", self.visit_data)
        self.assertIsInstance(self.visit_data["visit_end_time"], str)
        datetime.fromisoformat(self.visit_data["visit_end_time"].replace("Z", "+00:00")) 
        
        self.assertIn("events", self.visit_data)
        self.assertIsInstance(self.visit_data["events"], list)
        # self.assertTrue(len(self.visit_data["events"]) > 0) # Already checked in setUp

    def test_event_structure_and_types(self):
        for event in self.visit_data["events"]:
            self.assertIn("event_type", event)
            self.assertIsInstance(event["event_type"], str)
            self.assertIn("event_timestamp", event)
            self.assertIsInstance(event["event_timestamp"], str)
            datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00")) 

            event_type = event["event_type"]
            self.assertIn(event_type, ["page_view", "add_cart", "purchase"])
            self.assertIn(event_type, event, f"Event type key '{event_type}' missing in event data for event: {event}")


            if event_type == "page_view":
                pv_data = event["page_view"]
                self.assertIsInstance(pv_data["page_url"], str)
                if "referrer_url" in pv_data and pv_data["referrer_url"] is not None: # Referrer is optional
                    self.assertIsInstance(pv_data["referrer_url"], str)
            elif event_type == "add_cart":
                ac_data = event["add_cart"]
                self.assertIsInstance(ac_data["product_id"], str)
                self.assertIsInstance(ac_data["product_name"], str)
                self.assertIsInstance(ac_data["category"], str)
                self.assertIsInstance(ac_data["price"], float)
                self.assertIsInstance(ac_data["quantity"], int)
            elif event_type == "purchase":
                p_data = event["purchase"]
                self.assertIsInstance(p_data["order_id"], str)
                self.assertIsInstance(p_data["amount"], float)
                self.assertIsInstance(p_data["currency"], str)
                self.assertEqual(p_data["currency"], "USD")
                self.assertIsInstance(p_data["items"], list)
                self.assertTrue(len(p_data["items"]) > 0)
                for item in p_data["items"]:
                    self.assertIsInstance(item["product_id"], str)
                    self.assertIsInstance(item["product_name"], str)
                    self.assertIsInstance(item["category"], str)
                    self.assertIsInstance(item["price"], float)
                    self.assertIsInstance(item["quantity"], int)

    def test_event_timestamps_sequential(self):
        event_timestamps = [datetime.fromisoformat(e["event_timestamp"].replace("Z", "+00:00")) for e in self.visit_data["events"]]
        for i in range(len(event_timestamps) - 1):
            # Simulator adds random.randint(1, 60) seconds, so strictly increasing
            self.assertTrue(event_timestamps[i] < event_timestamps[i+1], 
                            f"Event timestamps not strictly sequential: {event_timestamps[i]} vs {event_timestamps[i+1]}")

    def test_visit_start_and_end_times(self):
        event_timestamps_str = [e["event_timestamp"] for e in self.visit_data["events"]]
        
        self.assertEqual(self.visit_data["visit_start_time"], event_timestamps_str[0])
        self.assertEqual(self.visit_data["visit_end_time"], event_timestamps_str[-1])
        
        start_dt = datetime.fromisoformat(self.visit_data["visit_start_time"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(self.visit_data["visit_end_time"].replace("Z", "+00:00"))
        event_dts = [datetime.fromisoformat(ts.replace("Z", "+00:00")) for ts in event_timestamps_str]
        
        self.assertEqual(start_dt, event_dts[0]) # First event
        self.assertEqual(end_dt, event_dts[-1]) # Last event
        self.assertTrue(start_dt <= end_dt)


    def test_add_cart_event_products(self):
        for event in self.visit_data["events"]:
            if event["event_type"] == "add_cart":
                ac_data = event["add_cart"]
                product_id = ac_data["product_id"]
                self.assertIn(product_id, self.products_map, f"Product ID {product_id} from add_cart not in loaded products.")
                
                original_product = self.products_map[product_id]
                self.assertEqual(ac_data["product_name"], original_product["product_name"])
                self.assertEqual(ac_data["category"], original_product["category"])
                self.assertEqual(ac_data["price"], float(original_product["price"])) 
                self.assertIsInstance(ac_data["quantity"], int)
                self.assertGreater(ac_data["quantity"], 0)

    def test_purchase_event_products_and_amount(self):
        for event in self.visit_data["events"]:
            if event["event_type"] == "purchase":
                p_data = event["purchase"]
                self.assertIsInstance(p_data["order_id"], str)
                self.assertIsInstance(p_data["amount"], float)
                self.assertEqual(p_data["currency"], "USD")

                calculated_total_amount = 0.0
                self.assertTrue(len(p_data["items"]) > 0, "Purchase event should have items.")
                for item in p_data["items"]:
                    product_id = item["product_id"]
                    self.assertIn(product_id, self.products_map, f"Product ID {product_id} from purchase items not in loaded products.")
                    
                    original_product = self.products_map[product_id]
                    self.assertEqual(item["product_name"], original_product["product_name"])
                    self.assertEqual(item["category"], original_product["category"])
                    self.assertEqual(item["price"], float(original_product["price"]))
                    self.assertIsInstance(item["quantity"], int)
                    self.assertGreater(item["quantity"], 0)
                    
                    calculated_total_amount += item["price"] * item["quantity"]
                
                self.assertAlmostEqual(p_data["amount"], calculated_total_amount, places=2)

    def test_data_conforms_to_schema_basic(self):
        for key in self.visit_data.keys():
            self.assertIn(key, self.schema_fields, f"Top-level key '{key}' not in schema.")

        for event in self.visit_data["events"]:
            self.assertIn("event_type", event)
            self.assertIn("event_timestamp", event)
            
            event_type_specific_key = event["event_type"] 
            self.assertIn(event_type_specific_key, event, f"Event type key '{event_type_specific_key}' missing in event data.")
            
            event_record_data = event[event_type_specific_key]
            self.assertIsNotNone(event_record_data, f"Event record data for '{event_type_specific_key}' is None.")

            if event_type_specific_key in self.event_schema_fields:
                schema_for_event_type = self.event_schema_fields[event_type_specific_key]
                for key_in_event_data in event_record_data.keys():
                    # referrer_url can be None and missing from schema if not explicitly defined as nullable
                    # The schema doesn't explicitly mark referrer_url as optional/nullable,
                    # but it's common for it to be. Test generator can produce None.
                    if key_in_event_data == "referrer_url" and event_record_data[key_in_event_data] is None:
                        continue 
                    self.assertIn(key_in_event_data, schema_for_event_type, 
                                  f"Key '{key_in_event_data}' in event '{event_type_specific_key}' not in schema definition {list(schema_for_event_type.keys())}.")
                
                if event_type_specific_key == "purchase":
                    self.assertIn("items", event_record_data)
                    purchase_items_data = event_record_data["items"]
                    self.assertIsInstance(purchase_items_data, list)
                    
                    schema_for_purchase_item = self.event_schema_fields.get('purchase_item')
                    self.assertIsNotNone(schema_for_purchase_item, "Schema for 'purchase_item' not found/parsed.")
                    
                    for item_data in purchase_items_data:
                        for key_in_item_data in item_data.keys():
                            self.assertIn(key_in_item_data, schema_for_purchase_item,
                                          f"Key '{key_in_item_data}' in purchase item not in schema definition {list(schema_for_purchase_item.keys())}.")
            else:
                self.fail(f"Unknown event type '{event_type_specific_key}' encountered or schema parsing issue for this event type.")

if __name__ == '__main__':
    unittest.main()
