import json
import os
from datetime import datetime

import pandas as pd


def read_jsonl_file(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                json_line = json.loads(line)
                data.append(json_line)
            except json.JSONDecodeError as e:
                continue
    return data


def process_experiment_data(experiment_path):
    # Initialize empty lists to hold data
    received_data = []
    sent_data = []
    peers_data = []

    # Iterate over each validator folder
    validator_dirs = [d for d in os.listdir(experiment_path) if os.path.isdir(os.path.join(experiment_path, d))]
    for validator in validator_dirs:
        validator_path = os.path.join(experiment_path, validator)

        # Process timed_received_bytes.jsonl
        received_file = os.path.join(validator_path, 'timed_received_bytes.jsonl')
        if os.path.exists(received_file):
            received = read_jsonl_file(received_file)
            for item in received:
                item['validator'] = validator
            received_data.extend(received)

        # Process timed_sent_bytes.jsonl
        sent_file = os.path.join(validator_path, 'timed_sent_bytes.jsonl')
        if os.path.exists(sent_file):
            sent = read_jsonl_file(sent_file)
            for item in sent:
                item['validator'] = validator
            sent_data.extend(sent)

        # Process peers.jsonl
        peers_file = os.path.join(validator_path, 'peers.jsonl')
        if os.path.exists(peers_file):
            peers = read_jsonl_file(peers_file)
            for item in peers:
                item['validator'] = validator
            peers_data.extend(peers)

    # Convert to DataFrames
    received_df = pd.json_normalize(received_data)
    sent_df = pd.json_normalize(sent_data)
    peers_df = pd.json_normalize(peers_data)

    return received_df, sent_df, peers_df
