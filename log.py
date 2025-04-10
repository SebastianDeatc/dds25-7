import json
import os
import threading

project_root = os.path.dirname(os.path.abspath(__file__))

# Build the absolute path to the logs.json file in the project root
log_file = os.path.join(project_root, 'logs.json')
print("Current working directory:", os.getcwd())

log_lock = threading.Lock()

def save_log(log):
    print('in save log')
    print(f"Log content: {log}")
    try:
        with open(log_file, 'w') as f:
            print('opened file')
            json.dump(log, f, indent=4)
    except Exception as e:
        print(f"Error saving log: {e}")