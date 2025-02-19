import json
from datetime import datetime

def load_json(file_path):
    """Read and parse JSON from a file."""
    try:
        with open(file_path, "r") as f:
            return [json.loads(line) for line in f.readlines()]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_json(file_path, data):
    """Write JSON data to a file."""
    with open(file_path, "a") as f:
        f.write(json.dumps(data) + "\n")

def format_timestamp():
    """Generate a current timestamp in ISO format."""
    return datetime.utcnow().isoformat()