import json
import os
import uuid  # For unique IDs
from tabulate import tabulate

class GraphDatabase:
    def __init__(self, file="db.json"):
        self.file = file
        if os.path.exists(self.file) and os.path.getsize(self.file) > 0:
            try:
                with open(self.file, "r") as f:
                    self.db = json.load(f)
            except json.JSONDecodeError:
                print("Warning: Invalid JSON format in db.json. Creating a new database.")
                self.db = {}
                self.save()
        else:
            self.db = {}
            self.save()

    def add_node(self, value):
        # Ensure value is a dictionary and has a 'name' key
        if not isinstance(value, dict) or "name" not in value:
            raise ValueError("Node value must be a dict with at least a 'name' key")

        # If 'age' is missing or None, replace it with the string "None"
        value["age"] = str(value.get("age", "None"))

        # Generate unique ID
        new_id = str(uuid.uuid4())
        self.db[new_id] = {"value": value, "edges": []}
        self.save()
        return new_id

    def add_edge(self, key1, key2):
        if key1 in self.db and key2 in self.db:
            self.db[key1]["edges"].append(key2)
            self.db[key2]["edges"].append(key1)
            self.save()

    def get_value(self, key):
        return self.db.get(key, {}).get("value", {})

    def delete_node(self, key):
        if key in self.db:
            for node in self.db[key]["edges"]:
                self.db[node]["edges"].remove(key)
            del self.db[key]
            self.save()

    def update_node(self, key, new_value):
        if key in self.db:
            if not isinstance(new_value, dict) or "name" not in new_value:
                raise ValueError("New value must be a dict with at least a 'name' key")
            new_value["age"] = str(new_value.get("age", "None"))
            self.db[key]["value"] = new_value
            self.save()
        else:
            print(f"Node with ID {key} not found.")



    def save(self):
        with open(self.file, "w") as f:
            json.dump(self.db, f, indent=4)

# Example Usage
db = GraphDatabase()
user1_id = db.add_node({"name": "Bin", "age": None})  # Will store age as "None"
user2_id = db.add_node({"name": "Bdkd", "age": 30})   # Will store age as 30
user3_id = db.add_node({"name": "Alex"})             # No age provided → Will store "None"
db.add_edge(user1_id, user2_id)

# Fetch values
user1 = db.get_value(user1_id)
user2 = db.get_value(user2_id)
user3 = db.get_value(user3_id)

# Ensure data is valid before printing
table = [["Name", "Age"]]
for user in [user1, user2, user3]:
    if "name" in user and "age" in user:
        table.append([user["name"], user["age"]])

# Print table
print(tabulate(table, headers="firstrow", tablefmt="grid"))
