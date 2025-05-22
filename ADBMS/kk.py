import json
import os
import uuid
import re
from tabulate import tabulate
from copy import deepcopy
import shutil
import shlex


class DatabaseManager:
    def __init__(self, registry_file="registry.json"):
        self.registry_file = registry_file
        self.registry = self._load_registry()
        self.current_db = None
        self.active_db_instance = None

        # Validate databases in registry
        invalid_dbs = []
        for db_name, db_file in list(self.registry.items()):
            nodes_file = db_file.replace(".json", "_nodes.json")
            indexes_file = db_file.replace(".json", "_indexes.json")

            # Check if either file exists
            if not (os.path.exists(nodes_file) or os.path.exists(indexes_file)):
                print(f"Warning: Database files for '{db_name}' not found. Removing from registry.")
                invalid_dbs.append(db_name)
                continue

            try:
                # Try to load the database to validate
                db_instance = GraphDatabase(db_file)
            except (json.JSONDecodeError, IOError, ValueError) as e:
                print(f"Warning: Failed to validate database '{db_name}': {e}. Removing from registry.")
                invalid_dbs.append(db_name)

        # Remove invalid databases
        for db_name in invalid_dbs:
            del self.registry[db_name]

        if invalid_dbs:
            self._save_registry()


    def _load_registry(self):
        if os.path.exists(self.registry_file) and os.path.getsize(self.registry_file) > 0:
            try:
                with open(self.registry_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                print("Warning: Invalid or inaccessible registry. Starting fresh.")
                return {}
        return {}

    def is_valid_uuid(val):
        """
        Validate if the given string is a valid UUID.

        Args:
            val (str): String to check.

        Returns:
            bool: True if val is a valid UUID, False otherwise.
        """
        try:
            uuid.UUID(val)
            return True
        except ValueError:
            return False

    def _save_registry(self):
        try:
            with open(self.registry_file, "w") as f:
                json.dump(self.registry, f, indent=4)
        except IOError as e:
            raise IOError(f"Failed to save registry: {e}")

    def create_database(self, db_name):
        if db_name in self.registry:
            raise ValueError(f"Database '{db_name}' already exists")
        file_name = f"{db_name}.json"
        self.registry[db_name] = file_name
        self._save_registry()
        db = GraphDatabase(file_name)
        print(f"Created database '{db_name}'")
        return db

    def delete_database(self, db_name):
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")
        file_name = self.registry[db_name]
        # Delete both nodes and indexes files
        nodes_file = file_name.replace(".json", "_nodes.json")
        indexes_file = file_name.replace(".json", "_indexes.json")
        if os.path.exists(nodes_file):
            os.remove(nodes_file)
        if os.path.exists(indexes_file):
            os.remove(indexes_file)
        del self.registry[db_name]
        self._save_registry()
        if self.current_db == db_name:
            self.current_db = None
            self.active_db_instance = None
        print(f"Deleted database '{db_name}'")

    def update_database(self, old_name, new_name):
        if old_name not in self.registry:
            raise ValueError(f"Database '{old_name}' does not exist")
        if new_name in self.registry:
            raise ValueError(f"Database '{new_name}' already exists")
        old_file = self.registry[old_name]
        new_file = f"{new_name}.json"

        # Rename both nodes and indexes files
        old_nodes = old_file.replace(".json", "_nodes.json")
        old_indexes = old_file.replace(".json", "_indexes.json")
        new_nodes = new_file.replace(".json", "_nodes.json")
        new_indexes = new_file.replace(".json", "_indexes.json")

        if os.path.exists(old_nodes):
            os.rename(old_nodes, new_nodes)
        if os.path.exists(old_indexes):
            os.rename(old_indexes, new_indexes)

        self.registry[new_name] = new_file
        del self.registry[old_name]
        self._save_registry()
        if self.current_db == old_name:
            self.current_db = new_name
        print(f"Renamed database '{old_name}' to '{new_name}'")

    def list_databases(self):
        return list(self.registry.keys())

    def use_database(self, db_name):
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")
        self.current_db = db_name
        self.active_db_instance = GraphDatabase(self.registry[db_name])
        return self.active_db_instance

    def backup_database(self, db_name, backup_file):
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")
        db_file = self.registry[db_name]
        nodes_file = db_file.replace(".json", "_nodes.json")
        indexes_file = db_file.replace(".json", "_indexes.json")

        if not os.path.exists(nodes_file):
            raise ValueError(f"Nodes file '{nodes_file}' not found")

        backup_nodes = backup_file.replace(".json", "_nodes.json")
        backup_indexes = backup_file.replace(".json", "_indexes.json")

        if os.path.exists(backup_nodes) or os.path.exists(backup_indexes):
            raise ValueError(f"Backup files already exist")

        try:
            shutil.copy2(nodes_file, backup_nodes)
            if os.path.exists(indexes_file):
                shutil.copy2(indexes_file, backup_indexes)
            print(f"Backed up database '{db_name}' to '{backup_nodes}' and '{backup_indexes}'")
        except IOError as e:
            raise IOError(f"Failed to create backup: {e}")

    def restore_database(self, db_name, backup_file):
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")

        backup_nodes = backup_file.replace(".json", "_nodes.json")
        backup_indexes = backup_file.replace(".json", "_indexes.json")

        if not os.path.exists(backup_nodes):
            raise ValueError(f"Backup nodes file '{backup_nodes}' does not exist")

        db_file = self.registry[db_name]
        nodes_file = db_file.replace(".json", "_nodes.json")
        indexes_file = db_file.replace(".json", "_indexes.json")

        try:
            # Validate JSON files
            with open(backup_nodes, "r") as f:
                json.load(f)
            if os.path.exists(backup_indexes):
                with open(backup_indexes, "r") as f:
                    json.load(f)

            # Perform the restore
            shutil.copy2(backup_nodes, nodes_file)
            if os.path.exists(backup_indexes):
                shutil.copy2(backup_indexes, indexes_file)
            else:
                if os.path.exists(indexes_file):
                    os.remove(indexes_file)

            if self.current_db == db_name:
                self.active_db_instance = GraphDatabase(db_file)
            print(f"Restored database '{db_name}' from backup")
        except json.JSONDecodeError:
            raise ValueError(f"Backup file is not valid JSON")
        except IOError as e:
            raise IOError(f"Failed to restore backup: {e}")

    def export_database(self, db_name, export_file):
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")
        db_file = self.registry[db_name]
        nodes_file = db_file.replace(".json", "_nodes.json")
        indexes_file = db_file.replace(".json", "_indexes.json")

        if not os.path.exists(nodes_file):
            raise ValueError(f"Nodes file '{nodes_file}' not found")

        export_nodes = export_file.replace(".json", "_nodes.json")
        export_indexes = export_file.replace(".json", "_indexes.json")

        if os.path.exists(export_nodes) or os.path.exists(export_indexes):
            raise ValueError(f"Export files already exist")

        try:
            with open(nodes_file, "r") as f:
                nodes_data = json.load(f)
            with open(export_nodes, "w") as f:
                json.dump(nodes_data, f, indent=4)

            if os.path.exists(indexes_file):
                with open(indexes_file, "r") as f:
                    indexes_data = json.load(f)
                with open(export_indexes, "w") as f:
                    json.dump(indexes_data, f, indent=4)

            print(f"Exported database '{db_name}' to '{export_nodes}' and '{export_indexes}'")
        except (json.JSONDecodeError, IOError) as e:
            raise ValueError(f"Failed to export database: {e}")

    def import_database(self, db_name, import_file, merge=False):
        if not import_file.endswith('.json'):
            raise ValueError("Import file must have a .json extension")
        if db_name not in self.registry:
            raise ValueError(f"Database '{db_name}' does not exist")

        import_nodes = import_file.replace(".json", "_nodes.json")
        import_indexes = import_file.replace(".json", "_indexes.json")

        if not os.path.exists(import_nodes):
            raise ValueError(f"Import nodes file '{import_nodes}' does not exist")

        db_file = self.registry[db_name]
        nodes_file = db_file.replace(".json", "_nodes.json")
        indexes_file = db_file.replace(".json", "_indexes.json")

        try:
            # Load import data
            with open(import_nodes, "r") as f:
                import_nodes_data = json.load(f)

            import_indexes_data = {}
            if os.path.exists(import_indexes):
                with open(import_indexes, "r") as f:
                    import_indexes_data = json.load(f)

            # Validate node structure
            for node_id, node_data in import_nodes_data.items():
                if not isinstance(node_data, dict) or "value" not in node_data or "edges" not in node_data:
                    raise ValueError(f"Invalid node structure for ID '{node_id}'")
                if not isinstance(node_data["value"], dict):
                    raise ValueError(f"Invalid value for node ID '{node_id}'")
                if not isinstance(node_data["edges"], dict):
                    raise ValueError(f"Invalid edges for node ID '{node_id}'")

                # Validate edge properties
                valid_edges = {}
                for target_id, props in node_data["edges"].items():
                    if not isinstance(props, dict):
                        continue
                    valid_props = {}
                    if "label" in props:
                        valid_props["label"] = str(props["label"])
                    if "weight" in props and isinstance(props["weight"], (int, float)):
                        valid_props["weight"] = props["weight"]
                    if set(props.keys()) - {"label", "weight"}:
                        print(f"Warning: Invalid edge properties in node {node_id}. Removed unrecognized properties.")
                    valid_edges[target_id] = valid_props
                import_nodes_data[node_id]["edges"] = valid_edges

            current_db = GraphDatabase(db_file)

            if merge:
                # Merge nodes
                for node_id, node_data in import_nodes_data.items():
                    if node_id not in current_db.db["nodes"]:
                        current_db.db["nodes"][node_id] = {"value": node_data["value"], "edges": {}}
                    else:
                        current_db.db["nodes"][node_id]["value"].update(node_data["value"])

                # Merge edges
                for node_id, node_data in import_nodes_data.items():
                    for target_id, edge_props in node_data["edges"].items():
                        if target_id in current_db.db["nodes"]:
                            current_db.db["nodes"][node_id]["edges"][target_id] = deepcopy(edge_props)
                            current_db.db["nodes"][target_id]["edges"][node_id] = deepcopy(edge_props)

                # Merge indexes
                for attr, value_dict in import_indexes_data.items():
                    if attr not in current_db.db["indexes"]:
                        current_db.db["indexes"][attr] = {}
                    for value_key, node_ids in value_dict.items():
                        if value_key not in current_db.db["indexes"][attr]:
                            current_db.db["indexes"][attr][value_key] = set()
                        current_db.db["indexes"][attr][value_key].update(node_ids)

                current_db.save()
            else:
                # Overwrite mode
                with open(nodes_file, "w") as f:
                    json.dump(import_nodes_data, f, indent=4)
                with open(indexes_file, "w") as f:
                    json.dump(import_indexes_data, f, indent=4)

            if self.current_db == db_name:
                self.active_db_instance = GraphDatabase(db_file)

            print(f"Imported database '{db_name}' from '{import_nodes}'{' with merge' if merge else ''}")
        except (json.JSONDecodeError, IOError) as e:
            raise ValueError(f"Failed to import database: {e}")

class GraphDatabase:
    def __init__(self, file="db.json"):
        self.file = file
        self.nodes_file = file.replace(".json", "_nodes.json")
        self.indexes_file = file.replace(".json", "_indexes.json")
        self.transaction = None
        self.transaction_history = []

        # Initialize nodes database
        if os.path.exists(self.nodes_file) and os.path.getsize(self.nodes_file) > 0:
            try:
                with open(self.nodes_file, "r") as f:
                    self.db = {"nodes": json.load(f), "indexes": {}}
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Invalid JSON format in {self.nodes_file}. Creating new nodes database.")
                self.db = {"nodes": {}, "indexes": {}}
                self.save_nodes()
        else:
            self.db = {"nodes": {}, "indexes": {}}
            self.save_nodes()

        # Initialize indexes database
        if os.path.exists(self.indexes_file) and os.path.getsize(self.indexes_file) > 0:
            try:
                with open(self.indexes_file, "r") as f:
                    indexes_data = json.load(f)
                    # Convert lists back to sets
                    self.db["indexes"] = {
                        attr: {k: set(v) for k, v in value_dict.items()}
                        for attr, value_dict in indexes_data.items()
                    }
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Invalid JSON format in {self.indexes_file}. Creating new indexes database.")
                self.db["indexes"] = {}
                self.save_indexes()
        else:
            self.db["indexes"] = {}
            self.save_indexes()

    def save_nodes(self):
        try:
            with open(self.nodes_file, "w") as f:
                json.dump(self.db["nodes"], f, indent=4)
        except IOError as e:
            raise IOError(f"Failed to save nodes database to {self.nodes_file}: {e}")

    def save_indexes(self):
        try:
            with open(self.indexes_file, "w") as f:
                json.dump(
                    {attr: {k: list(v) for k, v in value_dict.items()}
                     for attr, value_dict in self.db["indexes"].items()},
                    f, indent=4
                )
        except IOError as e:
            raise IOError(f"Failed to save indexes database to {self.indexes_file}: {e}")

    def save(self):
        self.save_nodes()
        self.save_indexes()

    # ... rest of the class methods remain the same ...

    def create_index(self, attribute):
        if not isinstance(attribute, str) or not attribute:
            raise ValueError("Index attribute must be a non-empty string")
        if attribute in self.db["indexes"]:
            raise ValueError(f"Index on '{attribute}' already exists")
        self.db["indexes"][attribute] = {}
        # Build index for existing nodes
        for node_id, data in self.db["nodes"].items():
            value = data["value"].get(attribute)
            if value is not None:
                # Store value as-is to support case-sensitive queries
                value_key = str(value)
                if value_key not in self.db["indexes"][attribute]:
                    self.db["indexes"][attribute][value_key] = set()
                self.db["indexes"][attribute][value_key].add(node_id)
        self.save()
        print(f"Created index on attribute '{attribute}'")

    def drop_index(self, attribute):
        if attribute not in self.db["indexes"]:
            raise ValueError(f"No index exists on '{attribute}'")
        del self.db["indexes"][attribute]
        self.save()
        print(f"Dropped index on attribute '{attribute}'")

    def list_indexes(self):
        return list(self.db["indexes"].keys())

    def _update_index(self, attribute, node_id, old_value, new_value):
        """Helper method to update index entries."""
        if attribute in self.db["indexes"]:
            # Remove old value from index
            if old_value is not None:
                old_value_key = str(old_value)
                if old_value_key in self.db["indexes"][attribute]:
                    self.db["indexes"][attribute][old_value_key].discard(node_id)
                    if not self.db["indexes"][attribute][old_value_key]:
                        del self.db["indexes"][attribute][old_value_key]
            # Add new value to index
            if new_value is not None:
                new_value_key = str(new_value)
                if new_value_key not in self.db["indexes"][attribute]:
                    self.db["indexes"][attribute][new_value_key] = set()
                self.db["indexes"][attribute][new_value_key].add(node_id)

    def add_node(self, value):
        if not isinstance(value, dict) or not value:
            raise ValueError("Node value must be a non-empty dict")
        if "name" in value:
            name = value["name"]
            if any(data["value"].get("name") == name for data in self.db["nodes"].values()):
                print(f"Warning: Node with name '{name}' already exists.")
        for key in value:
            if key != "name" and key.lower().startswith("na"):
                print(f"Warning: Key '{key}' is unusual. Did you mean 'name'?")
        for key, val in value.items():
            if not isinstance(val, (str, int, float, bool)):
                raise ValueError(f"Attribute '{key}' must be str, int, float, or bool, got {type(val)}")
        if self.transaction:
            self.transaction_history.append(deepcopy(self.db))
        new_id = str(uuid.uuid4())
        self.db["nodes"][new_id] = {"value": value, "edges": {}}
        # Update indexes
        for attribute in self.db["indexes"]:
            if attribute in value:
                self._update_index(attribute, new_id, None, value[attribute])
        self.save()
        return new_id

    def add_edge(self, key1, key2, label=None, weight=None):
        if key1 not in self.db["nodes"] or key2 not in self.db["nodes"]:
            raise KeyError("One or both node IDs do not exist")
        if key1 == key2:
            raise ValueError("Cannot add an edge from a node to itself")
        if key2 in self.db["nodes"][key1]["edges"]:
            raise ValueError(f"Edge between {key1[:8]}... and {key2[:8]}... already exists")
        if self.transaction:
            self.transaction_history.append(deepcopy(self.db))
        edge_props = {}
        if label is not None:
            edge_props["label"] = str(label)
        if weight is not None:
            if not isinstance(weight, (int, float)):
                raise ValueError("Weight must be a number")
            edge_props["weight"] = weight
        self.db["nodes"][key1]["edges"][key2] = edge_props
        self.db["nodes"][key2]["edges"][key1] = edge_props
        self.save()

    def delete_edge(self, key1, key2):
        if key1 not in self.db["nodes"] or key2 not in self.db["nodes"]:
            raise KeyError("One or both node IDs do not exist")
        if key2 not in self.db["nodes"][key1]["edges"]:
            raise ValueError(f"No edge exists between {key1[:8]}... and {key2[:8]}...")
        if self.transaction:
            self.transaction_history.append(deepcopy(self.db))
        del self.db["nodes"][key1]["edges"][key2]
        del self.db["nodes"][key2]["edges"][key1]
        self.save()

    def get_value(self, key):
        return self.db["nodes"].get(key, {}).get("value", {})

    def delete_node(self, key):
        if key not in self.db["nodes"]:
            raise KeyError(f"Node with ID {key} not found")
        if self.transaction:
            self.transaction_history.append(deepcopy(self.db))
        # Get the node's current values for index updates
        node_data = self.db["nodes"][key]
        # Update indexes by removing the node
        for attribute in self.db["indexes"]:
            value = node_data["value"].get(attribute)
            if value is not None:
                value_key = str(value).lower()
                if value_key in self.db["indexes"][attribute]:
                    self.db["indexes"][attribute][value_key].discard(key)
                    if not self.db["indexes"][attribute][value_key]:
                        del self.db["indexes"][attribute][value_key]
        # Remove all edges connected to this node
        for target_id in list(self.db["nodes"][key]["edges"].keys()):
            del self.db["nodes"][target_id]["edges"][key]
        # Delete the node
        del self.db["nodes"][key]
        self.save()

    def update_node(self, key, new_value):
        if key not in self.db["nodes"]:
            raise KeyError(f"Node with ID {key} not found")
        if not isinstance(new_value, dict) or not new_value:
            raise ValueError("New value must be a non-empty dict")
        if "name" in new_value:
            name = new_value["name"]
            if any(k != key and data["value"].get("name") == name for k, data in self.db["nodes"].items()):
                print(f"Warning: Node with name '{name}' already exists.")
        for attr, val in new_value.items():
            if not isinstance(val, (str, int, float, bool)):
                raise ValueError(f"Attribute '{attr}' must be str, int, float, or bool, got {type(val)}")
        if self.transaction:
            self.transaction_history.append(deepcopy(self.db))
        current_value = self.db["nodes"][key]["value"]
        # Update indexes
        for attribute in self.db["indexes"]:
            old_val = current_value.get(attribute)
            new_val = new_value.get(attribute, old_val)
            if old_val != new_val:
                self._update_index(attribute, key, old_val, new_val)
        current_value.update(new_value)
        self.db["nodes"][key]["value"] = current_value
        self.save()

    # def save(self):
    #     try:
    #         with open(self.file, "w") as f:
    #             json.dump(self.db, f, indent=4, default=lambda x: list(x) if isinstance(x, set) else x)
    #     except IOError as e:
    #         raise IOError(f"Failed to save database to {self.file}: {e}")

    def find_by_name(self, name):
        if not isinstance(name, str):
            raise ValueError("Name must be a string")

        # First try exact match using index if available
        if "name" in self.db["indexes"]:
            name_key = name.lower()  # Case-insensitive search
            return list(self.db["indexes"]["name"].get(name_key, set()))

        # Fallback to linear search if no name index
        matches = []
        for node_id, data in self.db["nodes"].items():
            node_name = data["value"].get("name")
            if node_name and name.lower() in node_name.lower():  # Case-insensitive contains
                matches.append(node_id)
        return matches

    def list_all_nodes(self):
        return {key: data["value"] for key, data in self.db["nodes"].items()}

    def find_path(self, start_key, end_key):
        if start_key not in self.db["nodes"] or end_key not in self.db["nodes"]:
            raise KeyError("One or both node IDs do not exist")
        if start_key == end_key:
            return [start_key]
        visited = set()
        queue = [(start_key, [start_key])]
        while queue:
            current_key, path = queue.pop(0)
            if current_key == end_key:
                return path
            if current_key not in visited:
                visited.add(current_key)
                for neighbor in self.db["nodes"][current_key]["edges"].keys():
                    if neighbor not in visited:
                        queue.append((neighbor, path + [neighbor]))
        return None

    def begin_transaction(self):
        if self.transaction is not None:
            raise ValueError("Transaction already in progress")
        self.transaction = True
        self.transaction_history = []
        print("Transaction started.")

    def commit_transaction(self):
        if self.transaction is None or not self.transaction:
            raise ValueError("No transaction in progress")
        if self.transaction_history:
            self.transaction_history.pop()
        print("Transaction committed. Last change is now permanent.")

    def rollback_transaction(self):
        if self.transaction is None or not self.transaction:
            raise ValueError("No transaction in progress")
        if not self.transaction_history:
            print("Nothing to rollback: No changes made in this transaction.")
            return
        self.db = self.transaction_history.pop()
        self.save()
        print("Rollback completed. Last change undone.")

    def stop_transaction(self):
        if self.transaction is None or not self.transaction:
            raise ValueError("No transaction in progress")
        self.transaction = False
        self.transaction_history = []
        print("Transaction stopped.")

    def query(self, query_string, cast_non_strings=False, case_sensitive=False):
        """
        Execute a query on the database to find nodes matching the given conditions.

        Args:
            query_string (str): Query starting with 'WHERE', supporting operators
                (=, >, <, >=, <=, !=, IN, CONTAINS, REGEX) and logical operators (AND, OR).
            cast_non_strings (bool): If True, cast non-string values to strings for CONTAINS queries.
            case_sensitive (bool): If True, perform case-sensitive comparisons for =, !=, IN, and CONTAINS.

        Returns:
            list: List of tuples containing (node_id, node_value) for matching nodes.

        Raises:
            ValueError: If the query is malformed or contains invalid conditions.
        """
        if not query_string.lower().startswith("where"):
            raise ValueError("Query must start with 'WHERE'")
        conditions_str = query_string[5:].strip()
        if not conditions_str:
            raise ValueError("No conditions provided in query")
        or_groups = re.split(r'\s+OR\s+', conditions_str, flags=re.IGNORECASE)
        or_conditions = []
        for or_group in or_groups:
            and_conditions = re.split(r'\s+AND\s+', or_group, flags=re.IGNORECASE)
            conditions = []
            for cond in and_conditions:
                cond = cond.strip()
                for op in ["=", ">", "<", ">=", "<=", "!="]:
                    if op in cond:
                        key, value = cond.split(op, 1)
                        key = key.strip()
                        value = value.strip()
                        if key.startswith("edge."):
                            edge_prop = key[5:]
                            if edge_prop == "weight":
                                try:
                                    value = float(value) if '.' in value else int(value)
                                except ValueError:
                                    raise ValueError(f"Invalid numeric value '{value}' for {key}")
                        conditions.append((key, op, value))
                        break
                else:
                    if " IN " in cond.upper():
                        key, value = cond.split(" IN ", 1)
                        key = key.strip()
                        value = value.strip()
                        if not (value.startswith("(") and value.endswith(")")):
                            raise ValueError(f"IN condition '{cond}' must use parentheses, e.g., (value1, value2)")
                        value_list = [v.strip() for v in value[1:-1].split(",") if v.strip()]
                        if not value_list:
                            raise ValueError(f"IN condition '{cond}' must have at least one value")
                        if key.startswith("edge."):
                            edge_prop = key[5:]
                            if edge_prop == "weight":
                                value_list = [float(v) if '.' in v else int(v) for v in value_list]
                        conditions.append((key, "IN", value_list))
                    elif " CONTAINS " in cond.upper():
                        key, value = cond.split(" CONTAINS ", 1)
                        key = key.strip()
                        value = value.strip()
                        conditions.append((key, "CONTAINS", value))
                    elif " REGEX " in cond.upper():
                        key, value = cond.split(" REGEX ", 1)
                        key = key.strip()
                        value = value.strip()
                        try:
                            re.compile(value)
                        except re.error:
                            raise ValueError(f"Invalid regex pattern in '{cond}'")
                        conditions.append((key, "REGEX", value))
                    else:
                        raise ValueError(f"Invalid condition: {cond}. Use =, >, <, >=, <=, !=, IN, CONTAINS, or REGEX")
            or_conditions.append(conditions)

        results = []
        seen_ids = set()
        for conditions in or_conditions:
            candidate_nodes = None
            for key, op, value in conditions:
                if key.startswith("edge."):
                    break
                if op not in ["=", "IN"] or key not in self.db["indexes"]:
                    candidate_nodes = None
                    break
                if op == "=":
                    value_key = str(value) if case_sensitive else str(value).lower()
                    nodes = self.db["indexes"][key].get(value_key, set())
                    if candidate_nodes is None:
                        candidate_nodes = nodes
                    else:
                        candidate_nodes = candidate_nodes.intersection(nodes)
                elif op == "IN":
                    nodes = set()
                    for val in value:
                        value_key = str(val) if case_sensitive else str(val).lower()
                        nodes.update(self.db["indexes"][key].get(value_key, set()))
                    if candidate_nodes is None:
                        candidate_nodes = nodes
                    else:
                        candidate_nodes = candidate_nodes.intersection(nodes)
            else:
                if candidate_nodes is not None:
                    for node_id in candidate_nodes:
                        if node_id in seen_ids:
                            continue
                        node_value = self.db["nodes"][node_id]["value"]
                        matches_and = True
                        for key, op, value in conditions:
                            if key not in node_value:
                                matches_and = False
                                break
                            node_val = node_value[key]
                            try:
                                if op == "=":
                                    if isinstance(node_val, (int, float)):
                                        value = float(value) if '.' in value else int(value)
                                    elif isinstance(node_val, bool):
                                        value = value.lower() == "true"
                                    else:
                                        value = value if case_sensitive else value.lower()
                                        node_val = node_val if case_sensitive else str(node_val).lower()
                                    matches_and = node_val == value
                                elif op == "IN":
                                    if isinstance(node_val, (int, float)):
                                        value = [float(v) if '.' in v else int(v) for v in value]
                                    elif isinstance(node_val, bool):
                                        value = [v.lower() == "true" for v in value]
                                    else:
                                        if not case_sensitive:
                                            node_val = str(node_val).lower()
                                            value = [v.lower() for v in value]
                                    matches_and = node_val in value
                            except ValueError:
                                raise ValueError(f"Value '{value}' cannot be compared with {key}'s type")
                            if not matches_and:
                                break
                        if matches_and:
                            results.append((node_id, node_value))
                            seen_ids.add(node_id)
                    continue

            # Full scan for non-indexed or edge conditions
            for node_id, data in self.db["nodes"].items():
                if node_id in seen_ids:
                    continue
                node_value = data["value"]
                edges = data["edges"]
                matches_and = True
                for key, op, value in conditions:
                    is_edge_condition = key.startswith("edge.")
                    if is_edge_condition:
                        edge_prop = key[5:]
                        if not edges:
                            matches_and = False
                            break
                        edge_matches = False
                        for edge_data in edges.values():
                            if edge_prop not in edge_data:
                                continue
                            edge_val = edge_data[edge_prop]
                            if op == "=":
                                if isinstance(edge_val, (int, float)):
                                    try:
                                        value_num = float(value) if '.' in value else int(value)
                                        edge_matches = edge_val == value_num
                                    except ValueError:
                                        edge_matches = False
                                else:
                                    edge_val = edge_val if case_sensitive else str(edge_val).lower()
                                    value = value if case_sensitive else value.lower()
                                    edge_matches = edge_val == value
                            elif op == ">" and isinstance(edge_val, (int, float)):
                                edge_matches = edge_val > (float(value) if '.' in value else int(value))
                            elif op == "<" and isinstance(edge_val, (int, float)):
                                edge_matches = edge_val < (float(value) if '.' in value else int(value))
                            elif op == ">=" and isinstance(edge_val, (int, float)):
                                edge_matches = edge_val >= (float(value) if '.' in value else int(value))
                            elif op == "<=" and isinstance(edge_val, (int, float)):
                                edge_matches = edge_val <= (float(value) if '.' in value else int(value))
                            elif op == "!=":
                                if isinstance(edge_val, (int, float)):
                                    try:
                                        value_num = float(value) if '.' in value else int(value)
                                        edge_matches = edge_val != value_num
                                    except ValueError:
                                        edge_matches = False
                                else:
                                    edge_val = edge_val if case_sensitive else str(edge_val).lower()
                                    value = value if case_sensitive else value.lower()
                                    edge_matches = edge_val != value
                            elif op == "IN":
                                if isinstance(edge_val, (int, float)):
                                    value_num = [float(v) if '.' in v else int(v) for v in value]
                                    edge_matches = edge_val in value_num
                                else:
                                    edge_val = edge_val if case_sensitive else str(edge_val).lower()
                                    value = [v if case_sensitive else v.lower() for v in value]
                                    edge_matches = edge_val in value
                            elif op == "CONTAINS":
                                edge_val_str = str(edge_val) if cast_non_strings else edge_val
                                if not isinstance(edge_val_str, str):
                                    matches_and = False
                                    break
                                if case_sensitive:
                                    edge_matches = value in edge_val_str
                                else:
                                    edge_matches = value.lower() in edge_val_str.lower()
                            elif op == "REGEX" and isinstance(edge_val, str):
                                edge_matches = bool(re.search(value, edge_val))
                            if edge_matches:
                                break
                        if not edge_matches:
                            matches_and = False
                            break
                    else:
                        if key not in node_value:
                            matches_and = False
                            break
                        node_val = node_value[key]
                        try:
                            if op == "=":
                                if isinstance(node_val, (int, float)):
                                    value = float(value) if '.' in value else int(value)
                                elif isinstance(node_val, bool):
                                    value = value.lower() == "true"
                                else:
                                    node_val = node_val if case_sensitive else str(node_val).lower()
                                    value = value if case_sensitive else value.lower()
                                matches_and = node_val == value
                            elif op == ">":
                                if not isinstance(node_val, (int, float)):
                                    matches_and = False
                                else:
                                    matches_and = node_val > (float(value) if '.' in value else int(value))
                            elif op == "<":
                                if not isinstance(node_val, (int, float)):
                                    matches_and = False
                                else:
                                    matches_and = node_val < (float(value) if '.' in value else int(value))
                            elif op == ">=":
                                if not isinstance(node_val, (int, float)):
                                    matches_and = False
                                else:
                                    matches_and = node_val >= (float(value) if '.' in value else int(value))
                            elif op == "<=":
                                if not isinstance(node_val, (int, float)):
                                    matches_and = False
                                else:
                                    matches_and = node_val <= (float(value) if '.' in value else int(value))
                            elif op == "!=":
                                if isinstance(node_val, (int, float)):
                                    value = float(value) if '.' in value else int(value)
                                elif isinstance(node_val, bool):
                                    value = value.lower() == "true"
                                else:
                                    node_val = node_val if case_sensitive else str(node_val).lower()
                                    value = value if case_sensitive else value.lower()
                                matches_and = node_val != value
                            elif op == "IN":
                                if isinstance(node_val, (int, float)):
                                    value = [float(v) if '.' in v else int(v) for v in value]
                                elif isinstance(node_val, bool):
                                    value = [v.lower() == "true" for v in value]
                                else:
                                    if not case_sensitive:
                                        node_val = str(node_val).lower()
                                        value = [v.lower() for v in value]
                                matches_and = node_val in value
                            elif op == "CONTAINS":
                                node_val_str = str(node_val) if cast_non_strings else node_val
                                if not isinstance(node_val_str, str):
                                    matches_and = False
                                else:
                                    if case_sensitive:
                                        matches_and = value in node_val_str
                                    else:
                                        matches_and = value.lower() in node_val_str.lower()
                            elif op == "REGEX":
                                if not isinstance(node_val, str):
                                    matches_and = False
                                else:
                                    matches_and = bool(re.search(value, node_val))
                        except ValueError:
                            raise ValueError(f"Value '{value}' cannot be compared with {key}'s type")
                        if not matches_and:
                            break
                if matches_and:
                    results.append((node_id, node_value))
                    seen_ids.add(node_id)
        return results

def print_db_state(db, message="Current Database State"):
    print(f"\n{message}:")
    table = [["ID", "Attributes", "Edges"]]
    for key, data in db.db["nodes"].items():
        attrs = ", ".join(f"{k}: {v}" for k, v in data["value"].items()) or "None"
        edges = ", ".join(f"{k[:8]}... ({v.get('label', '')} {v.get('weight', '')})".strip()
                          for k, v in data["edges"].items()) or "None"
        table.append([key[:8] + "...", attrs, edges])
    print(tabulate(table, headers="firstrow", tablefmt="grid"))


def is_valid_uuid(value):
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE
    )
    return bool(uuid_pattern.match(value))


def run_cli(manager):
    print("Welcome to the Graph Database CLI!")
    print(
        "Commands: create_db <name>, delete_db <name>, rename_db <old> <new>, list_dbs, use_db <name>, "
        "backup_db <name> <file>, restore_db <name> <file>, export_db <name> <file>, import_db <name> <file> [merge], "
        "add [key=value ...], connect <id1> <id2> [label=<label> weight=<weight>], "
        "disconnect <id1> <id2>, show <id>, update <id> [key=value ...], delete <id>, find <name>, "
        "query WHERE <condition> [CAST] [CASE_SENSITIVE] (supports =, >, <, >=, <=, !=, IN, CONTAINS, REGEX, AND, OR, edge.<property>), "
        "create_index <attribute>, drop_index <attribute>, list_indexes, "
        "list, path <id1> <id2>, begin, commit, rollback, stop, quit")
    db = None
    while True:
        cmd = input("Enter command: ").strip()
        if not cmd:
            print("Error: No command entered.")
            continue
        try:
            cmd_parts = shlex.split(cmd)
            if not cmd_parts:
                print("Error: No command entered.")
                continue
            command = cmd_parts[0].lower()
            args = cmd_parts[1:]  # List of arguments, preserving quoted strings
        except ValueError as e:
            print(f"Error: Invalid command syntax - {e}")
            continue

        try:
            if command == "quit":
                print("Exiting CLI...")
                break
            elif command == "create_db":
                if len(args) != 1:
                    raise ValueError("create_db requires exactly one database name")
                db_name = args[0]
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                manager.create_database(db_name)
            elif command == "delete_db":
                if len(args) != 1:
                    raise ValueError("delete_db requires exactly one database name")
                db_name = args[0]
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                manager.delete_database(db_name)
            elif command == "rename_db":
                if len(args) != 2:
                    raise ValueError("rename_db requires old and new database names")
                old_name, new_name = args
                if not re.match(r'^[a-zA-Z0-9_-]+$', old_name) or not re.match(r'^[a-zA-Z0-9_-]+$', new_name):
                    raise ValueError("Database names must contain only letters, numbers, underscores, or hyphens")
                manager.update_database(old_name, new_name)
            elif command == "list_dbs":
                if args:
                    raise ValueError("list_dbs takes no arguments")
                dbs = manager.list_databases()
                if dbs:
                    print("Databases:", ", ".join(dbs))
                else:
                    print("No databases exist")
            elif command == "use_db":
                if len(args) != 1:
                    raise ValueError("use_db requires exactly one database name")
                db_name = args[0]
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                db = manager.use_database(db_name)
                print(f"Switched to database '{db_name}'")
            elif command == "backup_db":
                if len(args) != 2:
                    raise ValueError("backup_db requires a database name and backup file")
                db_name, backup_file = args
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                if not backup_file.endswith('.json'):
                    raise ValueError("Backup file must have a .json extension")
                manager.backup_database(db_name, backup_file)
            elif command == "restore_db":
                if len(args) != 2:
                    raise ValueError("restore_db requires a database name and backup file")
                db_name, backup_file = args
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                if not backup_file.endswith('.json'):
                    raise ValueError("Backup file must have a .json extension")
                manager.restore_database(db_name, backup_file)
            elif command == "export_db":
                if len(args) != 2:
                    raise ValueError("export_db requires a database name and export file")
                db_name, export_file = args
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                if not export_file.endswith('.json'):
                    raise ValueError("Export file must have a .json extension")
                manager.export_database(db_name, export_file)
            elif command == "import_db":
                if len(args) < 2:
                    raise ValueError("import_db requires a database name and import file")
                db_name = args[0]
                import_file = args[1]
                merge = False
                if len(args) > 2 and args[2].lower() == "merge":
                    merge = True
                if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
                    raise ValueError("Database name must contain only letters, numbers, underscores, or hyphens")
                if not import_file.endswith('.json'):
                    raise ValueError("Import file must have a .json extension")
                manager.import_database(db_name, import_file, merge=merge)
            elif db is None:
                print("Error: No database selected. Use 'use_db <name>' to select a database first.")
            elif command == "create_index":
                if len(args) != 1:
                    raise ValueError("create_index requires exactly one attribute name")
                attribute = args[0]
                if not re.match(r'^[a-zA-Z0-9_-]+$', attribute):
                    raise ValueError("Attribute name must contain only letters, numbers, underscores, or hyphens")
                db.create_index(attribute)
            elif command == "drop_index":
                if len(args) != 1:
                    raise ValueError("drop_index requires exactly one attribute name")
                attribute = args[0]
                if not re.match(r'^[a-zA-Z0-9_-]+$', attribute):
                    raise ValueError("Attribute name must contain only letters, numbers, underscores, or hyphens")
                db.drop_index(attribute)
            elif command == "list_indexes":
                if args:
                    raise ValueError("list_indexes takes no arguments")
                indexes = db.list_indexes()
                if indexes:
                    print("Indexes:", ", ".join(indexes))
                else:
                    print("No indexes exist")
            elif command == "add":
                if not args:
                    raise ValueError("add requires at least one key=value pair")
                value = {}
                for arg in args:
                    if not re.match(r'^[a-zA-Z0-9_-]+=.*$', arg):
                        raise ValueError(f"Invalid argument '{arg}'. Must be key=value")
                    key, val = arg.split("=", 1)
                    if not key:
                        raise ValueError("Key cannot be empty")
                    if key in value:
                        print(f"Warning: Key '{key}' already set to '{value[key]}'. Overwriting with '{val}'.")
                    try:
                        if val.lower() == "true":
                            value[key] = True
                        elif val.lower() == "false":
                            value[key] = False
                        else:
                            try:
                                value[key] = int(val)
                            except ValueError:
                                try:
                                    value[key] = float(val)
                                except ValueError:
                                    value[key] = val
                    except ValueError:
                        value[key] = val
                node_id = db.add_node(value)
                print(f"Added node with ID: {node_id}")
            elif command == "connect":
                if len(args) < 2:
                    raise ValueError("connect requires at least two node IDs")
                key1, key2 = args[0], args[1]
                if not is_valid_uuid(key1) or not is_valid_uuid(key2):
                    raise ValueError("Node IDs must be valid UUIDs")
                label, weight = None, None
                for arg in args[2:]:
                    if not arg.startswith(("label=", "weight=")):
                        raise ValueError("Connect arguments must be label=<label> or weight=<weight>")
                    if arg.startswith("label="):
                        label = arg.split("=", 1)[1]
                        if not label:
                            raise ValueError("Label cannot be empty")
                    elif arg.startswith("weight="):
                        weight_str = arg.split("=", 1)[1]
                        try:
                            weight = float(weight_str)
                        except ValueError:
                            raise ValueError("Weight must be a number")
                db.add_edge(key1, key2, label=label, weight=weight)
                print(f"Connected {key1[:8]}... and {key2[:8]}...")
            elif command == "disconnect":
                if len(args) != 2:
                    raise ValueError("disconnect requires exactly two node IDs")
                key1, key2 = args
                if not is_valid_uuid(key1) or not is_valid_uuid(key2):
                    raise ValueError("Node IDs must be valid UUIDs")
                db.delete_edge(key1, key2)
                print(f"Disconnected {key1[:8]}... and {key2[:8]}...")
            elif command == "show":
                if len(args) != 1:
                    raise ValueError("show requires exactly one node ID")
                node_id = args[0]
                if not is_valid_uuid(node_id):
                    raise ValueError("Node ID must be a valid UUID")
                value = db.get_value(node_id)
                if value:
                    print(f"Node {node_id}: {value}")
                else:
                    print(f"Node {node_id[:8]}... not found")
            elif command == "update":
                if len(args) < 2:
                    raise ValueError("update requires a node ID and at least one key=value pair")
                node_id = args[0]
                if not is_valid_uuid(node_id):
                    raise ValueError("Node ID must be a valid UUID")
                value = {}
                for arg in args[1:]:
                    if not re.match(r'^[a-zA-Z0-9_-]+=.*$', arg):
                        raise ValueError(f"Invalid argument '{arg}'. Must be key=value")
                    key, val = arg.split("=", 1)
                    if not key:
                        raise ValueError("Key cannot be empty")
                    try:
                        if val.lower() == "true":
                            value[key] = True
                        elif val.lower() == "false":
                            value[key] = False
                        else:
                            try:
                                value[key] = int(val)
                            except ValueError:
                                try:
                                    value[key] = float(val)
                                except ValueError:
                                    value[key] = val
                    except ValueError:
                        value[key] = val
                db.update_node(node_id, value)
                print(f"Updated node {node_id[:8]}...")
            elif command == "delete":
                if len(args) != 1:
                    raise ValueError("delete requires exactly one node ID")
                node_id = args[0]
                if not is_valid_uuid(node_id):
                    raise ValueError("Node ID must be a valid UUID")
                db.delete_node(node_id)
                print(f"Deleted node {node_id[:8]}...")
            elif command == "find":
                if len(args) != 1:
                    raise ValueError("find requires exactly one name")
                name = args[0]
                if not name:
                    raise ValueError("Name cannot be empty")
                matches = db.find_by_name(name)
                if matches:
                    print(f"Found nodes with name '{name}':")
                    for id in matches:
                        print(f"ID {id}: {db.get_value(id)}")
                else:
                    print(f"No nodes found with name '{name}'")
            elif command == "query":
                if not args:
                    raise ValueError("query requires a WHERE clause")
                query_str = " ".join(args)
                if not query_str.lower().startswith("where"):
                    raise ValueError("Query must start with 'WHERE'")
                if not re.search(r'(=|>|<|>=|<=|!=| IN | CONTAINS | REGEX )', query_str, re.IGNORECASE):
                    raise ValueError(
                        "Query must contain a valid operator (=, >, <, >=, <=, !=, IN, CONTAINS, REGEX)")
                cast_non_strings = False
                case_sensitive = False
                query_parts = query_str.split()
                if query_parts[-1].upper() == "CAST":
                    cast_non_strings = True
                    query_str = " ".join(query_parts[:-1])
                elif query_parts[-1].upper() == "CASE_SENSITIVE":
                    case_sensitive = True
                    query_str = " ".join(query_parts[:-1])
                elif len(query_parts) > 1 and query_parts[-2].upper() == "CAST" and query_parts[
                    -1].upper() == "CASE_SENSITIVE":
                    cast_non_strings = True
                    case_sensitive = True
                    query_str = " ".join(query_parts[:-2])
                results = db.query(query_str, cast_non_strings=cast_non_strings, case_sensitive=case_sensitive)
                if results:
                    print(f"Query results for '{query_str}'{' (case-sensitive)' if case_sensitive else ''}:")
                    for node_id, value in results:
                        print(f"ID {node_id}: {value}")
                else:
                    print(f"No nodes match query '{query_str}'")
            elif command == "list":
                if args:
                    raise ValueError("list takes no arguments")
                all_nodes = db.list_all_nodes()
                if all_nodes:
                    print_db_state(db, "All Nodes in Database")
                else:
                    print("Database is empty")
            elif command == "path":
                if len(args) != 2:
                    raise ValueError("path requires exactly two node IDs")
                key1, key2 = args
                if not is_valid_uuid(key1) or not is_valid_uuid(key2):
                    raise ValueError("Node IDs must be valid UUIDs")
                path = db.find_path(key1, key2)
                if path:
                    print(
                        f"Path from {key1[:8]}... to {key2[:8]}...: {' -> '.join([db.get_value(k).get('name', 'N/A') for k in path])}")
                else:
                    print(f"No path found between {key1[:8]}... and {key2[:8]}...")
            elif command == "begin":
                if args:
                    raise ValueError("begin takes no arguments")
                db.begin_transaction()
            elif command == "commit":
                if args:
                    raise ValueError("commit takes no arguments")
                db.commit_transaction()
            elif command == "rollback":
                if args:
                    raise ValueError("rollback takes no arguments")
                db.rollback_transaction()
            elif command == "stop":
                if args:
                    raise ValueError("stop takes no arguments")
                db.stop_transaction()
            else:
                print(
                    "Invalid command. Use: create_db <name>, delete_db <name>, rename_db <old> <new>, list_dbs, use_db <name>, "
                    "backup_db <name> <file>, restore_db <name> <file>, export_db <name> <file>, import_db <name> <file> [merge], "
                    "add [key=value ...], connect <id1> <id2> [label=<label> weight=<weight>], "
                    "disconnect <id1> <id2>, show <id>, update <id> [key=value ...], delete <id>, find <name>, "
                    "query WHERE <condition> [CAST] [CASE_SENSITIVE] (supports =, >, <, >=, <=, !=, IN, CONTAINS, REGEX, AND, OR, edge.<property>), "
                    "create_index <attribute>, drop_index <attribute>, list_indexes, "
                    "list, path <id1> <id2>, begin, commit, rollback, stop, quit")
        except (ValueError, KeyError, IOError) as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    choice = input("Start fresh? (y/n): ").strip().lower()
    if choice == 'y':
        if os.path.exists("registry.json"):
            os.remove("registry.json")
            print("Cleared existing registry.")
        for db_file in [f for f in os.listdir() if f.endswith(".json") and f != "registry.json"]:
            os.remove(db_file)
            print(f"Cleared database file: {db_file}")
    manager = DatabaseManager()
    run_cli(manager)