import json
import os
from datetime import datetime
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

def read_jsonl_file(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                json_line = json.loads(line)
                data.append(json_line)
            except json.JSONDecodeError:
                continue
    return data

def process_validator_data(validator, experiment_path, ips_to_regions):
    validator_data = {
        'received_data': [],
        'sent_data': [],
        'peers_data': []
    }
    validator_path = os.path.join(experiment_path, validator)

    # Process timed_received_bytes.jsonl
    received_file = os.path.join(validator_path, 'timed_received_bytes.jsonl')
    if os.path.exists(received_file):
        received = read_jsonl_file(received_file)
        for item in received:
            item['validator'] = validator
            item['region'] = ips_to_regions.get(validator, 'Unknown')
        validator_data['received_data'].extend(received)

    # Process timed_sent_bytes.jsonl
    sent_file = os.path.join(validator_path, 'timed_sent_bytes.jsonl')
    if os.path.exists(sent_file):
        sent = read_jsonl_file(sent_file)
        for item in sent:
            item['validator'] = validator
            item['region'] = ips_to_regions.get(validator, 'Unknown')
        validator_data['sent_data'].extend(sent)

    # Process peers.jsonl
    peers_file = os.path.join(validator_path, 'peers.jsonl')
    if os.path.exists(peers_file):
        peers = read_jsonl_file(peers_file)
        for item in peers:
            item['validator'] = validator
            item['region'] = ips_to_regions.get(validator, 'Unknown')
        validator_data['peers_data'].extend(peers)

    return validator_data

def process_experiment_data(experiment_path, ips_to_regions):
    # Initialize empty lists to hold data
    received_data = []
    sent_data = []
    peers_data = []

    # Iterate over each validator folder
    validator_dirs = [
        d for d in os.listdir(experiment_path)
        if os.path.isdir(os.path.join(experiment_path, d))
    ]

    # Use multiprocessing Pool to parallelize processing of validator data
    with Pool(processes=cpu_count()) as pool:
        # Prepare arguments for the pool
        func = partial(process_validator_data, experiment_path=experiment_path, ips_to_regions=ips_to_regions)
        results = pool.map(func, validator_dirs)

    # Aggregate results from all processes
    for validator_data in results:
        received_data.extend(validator_data['received_data'])
        sent_data.extend(validator_data['sent_data'])
        peers_data.extend(validator_data['peers_data'])

    # Convert to DataFrames
    received_df = pd.json_normalize(received_data)
    sent_df = pd.json_normalize(sent_data)
    peers_df = pd.json_normalize(peers_data)

    # Process 'received_df'
    if not received_df.empty:
        received_df['msg.time'] = pd.to_datetime(received_df['msg.time'])
        received_df['target_ip'] = received_df['msg.ip_address'].str.split(':').str[0]
        received_df['target_region'] = received_df['target_ip'].map(ips_to_regions)
    else:
        print("No received data found.")

    # Process 'sent_df'
    if not sent_df.empty:
        sent_df['msg.time'] = pd.to_datetime(sent_df['msg.time'])
        sent_df['target_ip'] = sent_df['msg.ip_address'].str.split(':').str[0]
        sent_df['target_region'] = sent_df['target_ip'].map(ips_to_regions)
    else:
        print("No sent data found.")

    return received_df, sent_df, peers_df