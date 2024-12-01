import numpy as np
import pandas as pd

import os
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates

def plot_speed_progression_per_peer(speed_data, ips_to_regions, output_dir):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get unique validators (source peers)
    validators = speed_data['validator'].unique()

    for validator in validators:
        validator_data = speed_data[speed_data['validator'] == validator]
        plt.figure(figsize=(16, 9), dpi=480)

        # Get unique target peers for this validator
        target_peers = validator_data['msg.peer_id'].unique()

        for target_peer in target_peers:
            peer_data = validator_data[validator_data['msg.peer_id'] == target_peer]
            label = f"{peer_data['target_ip'].iloc[0]} ({peer_data['target_region'].iloc[0]})"
            plt.plot(peer_data['msg.time'], peer_data['speed_bps'], label=label)

        plt.title(f"Speed Progression for Validator {validator} ({ips_to_regions[validator]})")
        plt.xlabel('Time')
        plt.ylabel('Speed (Bytes/sec)')
        plt.legend(title='Target Peer (IP and Region)', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save the plot to the specified output directory
        output_file = os.path.join(output_dir, f"speed_progression_{validator}.png")
        plt.savefig(output_file, bbox_inches='tight')
        plt.close()

def calculate_speed_progression_per_peer(df, interval='10S'):
    # Convert timestamps to datetime if not already done
    df['msg.time'] = pd.to_datetime(df['msg.time'])

    # Set 'msg.time' as index
    df = df.set_index('msg.time')

    # Group data by validator (source peer), target peer (msg.peer_id), and 10-second intervals
    grouped = df.groupby([
        'validator',
        'msg.peer_id',
        pd.Grouper(freq=interval)
    ])

    # Sum the bytes in each interval
    speed_data = grouped['msg.bytes'].sum().reset_index()

    # Calculate speed in bytes per second
    speed_data['speed_bps'] = speed_data['msg.bytes'] / 10  # Since intervals are 10 seconds

    # Merge with target IP and region for labeling
    peer_info = df[['msg.peer_id', 'target_ip', 'target_region']].drop_duplicates()
    speed_data = speed_data.merge(peer_info, on='msg.peer_id', how='left')

    return speed_data

def calculate_speed_progression(df, action, interval='10S'):
    # Convert timestamps to datetime
    df['msg.time'] = pd.to_datetime(df['msg.time'])

    # Resample data for each validator into specified time intervals
    resampled = (
        df.set_index('msg.time')
        .groupby('validator')
        .resample(interval)
        .agg(
            total_bytes=('msg.bytes', 'sum'),
            region=('region', 'first')
        )
        .reset_index()
    )

    # Calculate elapsed time in seconds for each interval
    resampled['time_diff'] = resampled.groupby('validator')['msg.time'].diff().dt.total_seconds()

    # Calculate speed in Mbps
    resampled['speed_mbps'] = (resampled['total_bytes'] * 8) / (resampled['time_diff'] * 1_000_000)

    # Filter out invalid rows (e.g., NaN or infinite speeds due to zero time_diff)
    resampled = resampled.dropna(subset=['speed_mbps'])

    # Add action type (send/receive) for clarity in plotting
    resampled['action'] = action

    return resampled

def plot_speed_progression(sent_df, received_df, output_dir):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Calculate speed progression for sent and received data
    sent_speeds = calculate_speed_progression(sent_df, action='Send')
    received_speeds = calculate_speed_progression(received_df, action='Receive')

    received_speeds['validator_with_region'] = received_speeds['validator'] + ' (' + received_speeds['region'] + ')'
    sent_speeds['validator_with_region'] = sent_speeds['validator'] + ' (' + sent_speeds['region'] + ')'

    # Combine data for plotting
    progression = pd.concat([sent_speeds, received_speeds])

    # Plot the speed progression
    plt.figure(figsize=(16, 9), dpi=480)
    sns.lineplot(data=progression, x='msg.time', y='speed_mbps', hue='validator_with_region', style='action')
    plt.title('Speed Progression Over Time (Every 10 Seconds)')
    plt.xlabel('Time')
    plt.ylabel('Speed (Mbps)')
    plt.legend(title='Validator and Action', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    output_file = os.path.join(output_dir, f"speed_progression_for_all_peers.png")
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()

def calculate_total_speeds(sent_df, received_df):
    # Convert timestamps to datetime
    sent_df['msg.time'] = pd.to_datetime(sent_df['msg.time'])
    received_df['msg.time'] = pd.to_datetime(received_df['msg.time'])

    # Calculate total bytes sent and received
    sent_agg = sent_df.groupby('validator').agg(
        total_bytes_sent=('msg.bytes', 'sum'),
        start_time_sent=('msg.time', 'min'),
        end_time_sent=('msg.time', 'max'),
        region = ('region', 'first')
    ).reset_index()

    received_agg = received_df.groupby('validator').agg(
        total_bytes_received=('msg.bytes', 'sum'),
        start_time_received=('msg.time', 'min'),
        end_time_received=('msg.time', 'max'),
        region = ('region', 'first')
    ).reset_index()

    # Merge sent and received data
    total_speeds = pd.merge(sent_agg, received_agg, on='validator', how='outer')

    # Calculate total time in seconds for sending and receiving
    total_speeds['total_time_sent'] = (total_speeds['end_time_sent'] - total_speeds['start_time_sent']).dt.total_seconds()
    total_speeds['total_time_received'] = (total_speeds['end_time_received'] - total_speeds['start_time_received']).dt.total_seconds()

    # Calculate upload and download speeds in Mbps
    total_speeds['upload_speed_mbps'] = (total_speeds['total_bytes_sent'] * 8) / (total_speeds['total_time_sent'] * 1_000_000)
    total_speeds['download_speed_mbps'] = (total_speeds['total_bytes_received'] * 8) / (total_speeds['total_time_received'] * 1_000_000)

    return total_speeds

def calculate_total_regions_speeds(sent_df, received_df):
    # Convert timestamps to datetime
    sent_df['msg.time'] = pd.to_datetime(sent_df['msg.time'])
    received_df['msg.time'] = pd.to_datetime(received_df['msg.time'])

    # Calculate total bytes sent and received
    sent_agg = sent_df.groupby('region').agg(
        total_bytes_sent=('msg.bytes', 'sum'),
        start_time_sent=('msg.time', 'min'),
        end_time_sent=('msg.time', 'max')
    ).reset_index()

    received_agg = received_df.groupby('region').agg(
        total_bytes_received=('msg.bytes', 'sum'),
        start_time_received=('msg.time', 'min'),
        end_time_received=('msg.time', 'max')
    ).reset_index()

    # Merge sent and received data
    total_speeds = pd.merge(sent_agg, received_agg, on='region', how='outer')

    # Calculate total time in seconds for sending and receiving
    total_speeds['total_time_sent'] = (total_speeds['end_time_sent'] - total_speeds['start_time_sent']).dt.total_seconds()
    total_speeds['total_time_received'] = (total_speeds['end_time_received'] - total_speeds['start_time_received']).dt.total_seconds()

    # Calculate upload and download speeds in Mbps
    total_speeds['upload_speed_mbps'] = (total_speeds['total_bytes_sent'] * 8) / (total_speeds['total_time_sent'] * 1_000_000)
    total_speeds['download_speed_mbps'] = (total_speeds['total_bytes_received'] * 8) / (total_speeds['total_time_received'] * 1_000_000)

    return total_speeds

def plot_speeds(total_speeds, output_dir):
    total_speeds['validator_with_region'] = total_speeds['validator'] + ' (' + total_speeds['region_x'] + ')'
    # Melt data for plotting
    plot_data = total_speeds.melt(id_vars=['validator_with_region'], value_vars=['upload_speed_mbps', 'download_speed_mbps'],
                                  var_name='Speed Type', value_name='Speed (Mbps)')

    # Create bar plot
    plt.figure(figsize=(16, 9), dpi=480)
    sns.barplot(data=plot_data, x='validator_with_region', y='Speed (Mbps)', hue='Speed Type')
    plt.title('Upload vs Download Speeds for Each Validator')
    plt.xlabel('Validator with region')
    plt.ylabel('Speed (Mbps)')
    plt.xticks(rotation=45)
    plt.legend(title='Speed Type')
    output_file = os.path.join(output_dir, f"upload_vs_download_speed_for_each_validator.png")
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()

def plot_region_speeds(total_speeds, output_dir):
    # Melt data for plotting
    plot_data = total_speeds.melt(id_vars=['region'], value_vars=['upload_speed_mbps', 'download_speed_mbps'],
                                  var_name='Region', value_name='Speed (Mbps)')

    # Create bar plot
    plt.figure(figsize=(16, 9), dpi=480)
    sns.barplot(data=plot_data, x='region', y='Speed (Mbps)', hue='Region')
    plt.title('Upload vs Download Speeds for Each Region')
    plt.xlabel('Region')
    plt.ylabel('Speed (Mbps)')
    plt.xticks(rotation=45)
    plt.legend(title='Speed Type')
    output_file = os.path.join(output_dir, f"upload_vs_download_per_region.png")
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()

def calculate_mean_speeds(total_speeds):
    mean_upload_speed = total_speeds['upload_speed_mbps'].mean()
    mean_download_speed = total_speeds['download_speed_mbps'].mean()
    return mean_upload_speed, mean_download_speed

def compute_speeds(df, action):
    # Convert timestamps to datetime
    df['msg.time'] = pd.to_datetime(df['msg.time'])

    # Compute time differences between consecutive messages from the same peer
    df['time_diff'] = df.groupby(['validator', 'msg.peer_id'])['msg.time'].diff().dt.total_seconds()

    # Use 'msg.bytes' directly as the amount of bytes transferred in each interval
    # Compute speed (bytes per second)
    df['speed_bytes_per_sec'] = df['msg.bytes'] / df['time_diff']

    # Remove NaN or infinite values resulting from any division issues
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['speed_bytes_per_sec'])

    # Convert speed to megabits per second (Mbps)
    df['speed_mbps'] = (df['speed_bytes_per_sec'] * 8) / 1_000_000  # Using underscore for readability

    df['action'] = action
    return df

def resample_speeds(speeds_df, interval='1T'):
    # Ensure 'msg.time' is a datetime object and set it as the index
    speeds_df = speeds_df.set_index('msg.time')

    # Resample the data and compute the mean
    resampled_df = speeds_df.resample(interval).mean().reset_index()

    return resampled_df
