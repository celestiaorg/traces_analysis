import json
import os
from datetime import datetime
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

def process_chunk(chunk):
    return pd.json_normalize(chunk)

def read_jsonl_file(file_path):
    data = []
    print(file_path)
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

    return validator_data

def process_chunk_dataframe(args):
    chunk, func_name, ips_to_regions = args
    if func_name == 'process_received_chunk':
        return process_received_chunk(chunk, ips_to_regions)
    elif func_name == 'process_sent_chunk':
        return process_sent_chunk(chunk, ips_to_regions)
    else:
        raise ValueError(f"Unknown function name: {func_name}")


def parallel_process_dataframe(df, func_name, ips_to_regions):
    num_processes = cpu_count()-1
    chunk_size = int(len(df) / num_processes) + 1
    df_chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    # Prepare arguments
    args_list = [(chunk, func_name, ips_to_regions) for chunk in df_chunks]

    with Pool(processes=num_processes) as pool:
        result = pool.map(process_chunk_dataframe, args_list)

    # Concatenate the processed chunks
    processed_df = pd.concat(result, ignore_index=True)
    return processed_df

def process_received_chunk(chunk, ips_to_regions):
    chunk['msg.time'] = pd.to_datetime(chunk['msg.time'])
    chunk['target_ip'] = chunk['msg.ip_address'].str.split(':').str[0]
    chunk['target_region'] = chunk['target_ip'].map(ips_to_regions)
    return chunk


def process_sent_chunk(chunk, ips_to_regions):
    chunk['msg.time'] = pd.to_datetime(chunk['msg.time'])
    chunk['target_ip'] = chunk['msg.ip_address'].str.split(':').str[0]
    chunk['target_region'] = chunk['target_ip'].map(ips_to_regions)
    return chunk

def process_experiment_data(experiment_path, ips_to_regions):
    # Initialize empty lists to hold data
    received_data = []
    sent_data = []

    # Iterate over each validator folder
    validator_dirs = [
        d for d in os.listdir(experiment_path)
        if os.path.isdir(os.path.join(experiment_path, d))
    ]

    # Use multiprocessing Pool to parallelize processing of validator data
    with Pool(processes=cpu_count()-1) as pool:
        # Prepare arguments for the pool
        func = partial(process_validator_data, experiment_path=experiment_path, ips_to_regions=ips_to_regions)
        results = pool.map(func, validator_dirs)

    print("aggregating results")
    # Aggregate results from all processes
    for validator_data in results:
        received_data.extend(validator_data['received_data'])
        sent_data.extend(validator_data['sent_data'])

    print("normalizing files...")
    # Convert to DataFrames
    received_df = parallel_json_normalize(received_data)
    print("normalised received_df")
    sent_df = parallel_json_normalize(sent_data)
    print("normalised sent_df")


    # Process 'received_df'
    if not received_df.empty:
        received_df = parallel_process_dataframe(received_df, 'process_received_chunk', ips_to_regions)
    else:
        print("No received data found.")
    print("processed received_df")

    # Process 'sent_df'
    if not sent_df.empty:
        sent_df = parallel_process_dataframe(sent_df, 'process_sent_chunk', ips_to_regions)
    else:
        print("No sent data found.")
    print("processed sent_df")

    return received_df, sent_df

def parallel_json_normalize(data_list):
    # Determine the number of processes to use
    num_processes = cpu_count()-1

    # Split the data into chunks
    chunk_size = int(len(data_list) / num_processes) + 1
    data_chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

    with Pool(processes=num_processes) as pool:
        result = pool.map(process_chunk, data_chunks)

    # Concatenate the results
    df = pd.concat(result, ignore_index=True)
    return df