import json

def generate_product_pages(json_file_path):
    """
    Generates a list of product page URLs from a JSON file containing product data.

    Args:
        json_file_path (str): The path to the JSON file containing product data.

    Returns:
        list: A list of product page URLs.
    """
    try:
        with open(json_file_path, 'r') as f:
            products = json.load(f)
    except FileNotFoundError:
        return ["Error: products.json not found."]
    except json.JSONDecodeError:
        return ["Error: Invalid JSON format in products.json."]

    product_pages = []
    for product in products:
        if 'product_id' in product:  # Check if 'id' field exists
            product_pages.append(f"https://example.com/products/{product['product_id']}")
        else:
            print(f"Warning: Product missing 'product_id' field: {product}")

    return product_pages

# Example usage:
json_file = "/Users/adam/projects/datasolutionslab/Task3/products.json"  # Replace with the actual path to your products.json file.
pages = generate_product_pages(json_file)

for page in pages:
    print(page)
