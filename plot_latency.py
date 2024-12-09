import json
import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re

from parse_validators_regions import parse_ip_to_region


def read_jsonl_file(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                json_line = json.loads(line)
                data.append(json_line)
            except json.JSONDecodeError:
                # If there's an invalid line, skip it
                continue
    return data


def parse_time(timestr):
    timestr = timestr.replace('Z', '+00:00')

    # Example input: "2024-12-09T20:03:09.030144354+00:00"
    # We'll truncate the fraction before using strptime.

    if '.' in timestr:
        main_part, frac_tz = timestr.split('.')
        # main_part: "2024-12-09T20:03:09"
        # frac_tz: "030144354+00:00"

        # Separate fraction from timezone
        sign_index = frac_tz.find('+')
        if sign_index == -1:
            sign_index = frac_tz.find('-')

        if sign_index != -1:
            fraction = frac_tz[:sign_index]
            tz = frac_tz[sign_index:]
        else:
            fraction = frac_tz
            tz = ''

        fraction = fraction[:6]  # Truncate to microseconds
        new_timestr = f"{main_part}.{fraction}{tz}"
    else:
        new_timestr = timestr

    # Now we know the format is something like: "%Y-%m-%dT%H:%M:%S.%f%z"
    return datetime.strptime(new_timestr, "%Y-%m-%dT%H:%M:%S.%f%z")

def process_msg_latency_file(file_path, ip_to_region):
    # Reads a msg_latency.jsonl file and returns a DataFrame with:
    # columns: validator, peer_id, ip_address (peer), region, latency_ms
    data = []
    if not os.path.exists(file_path):
        return pd.DataFrame()

    start = 10000
    end = 30000
    with open(file_path, 'r') as f:
        count = 0
        for line in f:
            count = count +1
            if count < start :
                continue
            if count > end:
                break

            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue


            msg = entry.get('msg', {})
            validator = entry.get('node_id')
            ip_address = msg.get('ip_address')
            send_time_str = msg.get('send_time')
            receive_time_str = msg.get('receive_time')
            peer_id = msg.get('peer_id')

            if not (validator and ip_address and send_time_str and receive_time_str and peer_id):
                continue

            send_time = parse_time(send_time_str)
            receive_time = parse_time(receive_time_str)
            latency = (receive_time - send_time).total_seconds() * 1000.0  # latency in ms

            region = ip_to_region.get(ip_address, 'Unknown')

            data.append({
                'validator': validator,
                'ip_address': ip_address,
                'region': region,
                'latency_ms': latency
            })

    df = pd.DataFrame(data)
    return df

def plot_mean_latency_per_validator(df, validator_ip, ips_to_region, output_dir):
    # Group by peer_id to get mean latency for each peer
    if df.empty:
        print(f"No data found for validator {validator_ip}")
        return

    agg_df = df.groupby(['ip_address', 'region'], as_index=False)['latency_ms'].mean()
    # Create a label combining peer_id and region for readability
    agg_df['peer_label'] = agg_df['ip_address'] + ' (' + agg_df['region'] + ')'
    agg_df = agg_df.sort_values('latency_ms')

    plt.figure(figsize=(12, 6))
    sns.barplot(data=agg_df, x='peer_label', y='latency_ms', palette='viridis')
    validator_label = validator_ip + ' (' + ips_to_region[validator_ip] + ')'
    plt.title(f"Mean Latency for Validator {validator_label}")
    plt.xlabel("Peer (Region)")
    plt.ylabel("Latency (ms)")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    output_file = os.path.join(output_dir, f"mean_latency_{validator_ip}.png")
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()
    print(f"Plot saved for {validator_ip} at {output_file}")

if __name__ == "__main__":
    # Set your root experiment directory and regions file
    experiment_name = "traces_tm_latency"
    experiment_path = os.path.join(".", experiment_name)
    regions_file = "list.txt"  # adjust path as needed
    output_dir = "plots/" + experiment_name
    os.makedirs(output_dir, exist_ok=True)

    # Parse IP-to-region mapping
    ip_to_region = parse_ip_to_region(regions_file)

    # Iterate over each IP directory in the experiment
    # Each IP directory has a msg_latency.jsonl file
    validator_ips = [d for d in os.listdir(experiment_path) if os.path.isdir(os.path.join(experiment_path, d))]

    separate_plots_dir = "plots/" + experiment_name +"/separate"
    os.makedirs(separate_plots_dir, exist_ok=True)
    for validator_ip in validator_ips:
        msg_latency_file = os.path.join(experiment_path, validator_ip, "msg_latency.jsonl")
        df = process_msg_latency_file(msg_latency_file, ip_to_region)
        # Produce a bar plot for this validator
        plot_mean_latency_per_validator(df, validator_ip,ip_to_region, separate_plots_dir)

