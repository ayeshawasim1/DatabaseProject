from flask import Flask, request, jsonify, send_from_directory, render_template
from kk import DatabaseManager, GraphDatabase
import os
import json
import uuid
import shutil

app = Flask(__name__)
manager = DatabaseManager()

# Serve the main page
@app.route('/')
def index():
    return render_template('index.html')

# Database operations
@app.route('/api/databases', methods=['GET'])
def list_databases():
    return jsonify(manager.list_databases())

@app.route('/api/databases', methods=['POST'])
def create_database():
    data = request.json
    if 'name' not in data:
        return jsonify({'error': 'Database name required'}), 400
    try:
        db = manager.create_database(data['name'])
        return jsonify({'message': f"Database '{data['name']}' created"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/databases/<db_name>', methods=['DELETE'])
def delete_database(db_name):
    try:
        manager.delete_database(db_name)
        return jsonify({'message': f"Database '{db_name}' deleted"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/databases/<db_name>', methods=['PUT'])
def use_database(db_name):
    try:
        db = manager.use_database(db_name)
        return jsonify({'message': f"Using database '{db_name}'"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Node operations
@app.route('/api/nodes', methods=['POST'])
def add_node():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        node_id = manager.active_db_instance.add_node(data)
        return jsonify({'id': node_id, 'message': 'Node added'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/nodes/<node_id>', methods=['GET'])
def get_node(node_id):
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        value = manager.active_db_instance.get_value(node_id)
        if not value:
            return jsonify({'error': 'Node not found'}), 404
        return jsonify(value)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/nodes/<node_id>', methods=['PUT'])
def update_node(node_id):
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        manager.active_db_instance.update_node(node_id, data)
        return jsonify({'message': 'Node updated'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/nodes/<node_id>', methods=['DELETE'])
def delete_node(node_id):
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.delete_node(node_id)
        return jsonify({'message': 'Node deleted'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Edge operations
@app.route('/api/edges', methods=['POST'])
def add_edge():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        manager.active_db_instance.add_edge(
            data['source'],
            data['target'],
            label=data.get('label'),
            weight=data.get('weight')
        )
        return jsonify({'message': 'Edge added'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/edges', methods=['DELETE'])
def delete_edge():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        manager.active_db_instance.delete_edge(data['source'], data['target'])
        return jsonify({'message': 'Edge deleted'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Query operations
@app.route('/api/query', methods=['POST'])
def query():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        results = manager.active_db_instance.query(
            data['query'],
            cast_non_strings=data.get('cast_non_strings', False),
            case_sensitive=data.get('case_sensitive', False)
        )
        return jsonify([{'id': r[0], 'value': r[1]} for r in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Index operations
@app.route('/api/indexes', methods=['GET'])
def list_indexes():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        return jsonify(manager.active_db_instance.list_indexes())
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/indexes', methods=['POST'])
def create_index():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        manager.active_db_instance.create_index(data['attribute'])
        return jsonify({'message': f"Index on '{data['attribute']}' created"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/indexes/<attribute>', methods=['DELETE'])
def drop_index(attribute):
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.drop_index(attribute)
        return jsonify({'message': f"Index on '{attribute}' dropped"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Path finding
@app.route('/api/path', methods=['POST'])
def find_path():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    data = request.json
    try:
        path = manager.active_db_instance.find_path(data['source'], data['target'])
        if path:
            return jsonify({'path': path})
        return jsonify({'message': 'No path found'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Transaction operations
@app.route('/api/transaction', methods=['POST'])
def begin_transaction():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.begin_transaction()
        return jsonify({'message': 'Transaction started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/transaction/commit', methods=['POST'])
def commit_transaction():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.commit_transaction()
        return jsonify({'message': 'Transaction committed'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/transaction/rollback', methods=['POST'])
def rollback_transaction():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.rollback_transaction()
        return jsonify({'message': 'Transaction rolled back'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/transaction/stop', methods=['POST'])
def stop_transaction():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        manager.active_db_instance.stop_transaction()
        return jsonify({'message': 'Transaction stopped'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/databases/<old_name>/rename', methods=['PUT'])
def rename_database(old_name):
    data = request.json
    if 'new_name' not in data:
        return jsonify({'error': 'New database name required'}), 400
    try:
        manager.update_database(old_name, data['new_name'])
        if manager.current_db == old_name:
            manager.use_database(data['new_name'])
        return jsonify({'message': f"Database renamed from '{old_name}' to '{data['new_name']}'"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Backup endpoint
@app.route('/api/backup', methods=['POST'])
def backup_database():
    data = request.json
    if 'db_name' not in data or 'backup_file' not in data:
        return jsonify({'error': 'Database name and backup file required'}), 400
    try:
        manager.backup_database(data['db_name'], data['backup_file'])
        return jsonify({'message': f"Database '{data['db_name']}' backed up to '{data['backup_file']}'"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Restore endpoint
@app.route('/api/restore', methods=['POST'])
def restore_database():
    data = request.json
    if 'db_name' not in data or 'backup_file' not in data:
        return jsonify({'error': 'Database name and backup file required'}), 400
    try:
        manager.restore_database(data['db_name'], data['backup_file'])
        return jsonify({'message': f"Database '{data['db_name']}' restored from '{data['backup_file']}'"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Export endpoint
@app.route('/api/export', methods=['POST'])
def export_database():
    data = request.json
    if 'db_name' not in data or 'export_file' not in data:
        return jsonify({'error': 'Database name and export file required'}), 400
    try:
        manager.export_database(data['db_name'], data['export_file'])
        return jsonify({'message': f"Database '{data['db_name']}' exported to '{data['export_file']}'"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Import endpoint
@app.route('/api/import', methods=['POST'])
def import_database():
    data = request.json
    if 'db_name' not in data or 'import_file' not in data:
        return jsonify({'error': 'Database name and import file required'}), 400
    try:
        merge = data.get('merge', False)
        manager.import_database(data['db_name'], data['import_file'], merge=merge)
        return jsonify({'message': f"Database '{data['db_name']}' imported from '{data['import_file']}'{' with merge' if merge else ''}"})
    except Exception as e:
        return jsonify({'error': str(e)}), 400



@app.route('/api/nodes/find/<name>', methods=['GET'])
def find_nodes_by_name(name):
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        matches = manager.active_db_instance.find_by_name(name)
        results = []
        for node_id in matches:
            value = manager.active_db_instance.get_value(node_id)
            results.append({'id': node_id, 'value': value})
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 400


# Node list endpoint
@app.route('/api/nodes', methods=['GET'])
def list_nodes():
    if not manager.current_db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        # Fetch all nodes with their full data (value and edges)
        nodes = manager.active_db_instance.db['nodes']
        # Transform the data to include both value and edges
        result = {
            node_id: {
                'value': data['value'],
                'edges': data['edges']
            }
            for node_id, data in nodes.items()
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Serve static files (CSS, JS)
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True, port=5000)