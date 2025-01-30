import json

with open("./service_account_detail.json", "r") as f:
    try:
        data = json.load(f)
        print("JSON is valid.")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")