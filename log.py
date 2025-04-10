import json

import os
project_root = os.path.dirname(os.path.abspath(__file__))

# Build the absolute path to the logs.json file in the project root
log_file = os.path.join(project_root, 'logs.json')
print("Current working directory:", os.getcwd())

def save_log(log):
    print('in save log')
    with open(log_file, 'w') as f:
        print('opened file')
        json.dump(log, f, indent=4)