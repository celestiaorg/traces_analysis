import numpy as np
import pandas as pd

import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

def calculate_total_speeds(sent_df, received_df):
    # Convert timestamps to datetime
    sent_df['msg.time'] = pd.to_datetime(sent_df['msg.time'])
    received_df['msg.time'] = pd.to_datetime(received_df['msg.time'])

    # Calculate total bytes sent and received
    sent_agg = sent_df.groupby('validator').agg(
        total_bytes_sent=('msg.bytes', 'sum'),
        start_time_sent=('msg.time', 'min'),
        end_time_sent=('msg.time', 'max')
    ).reset_index()

    received_agg = received_df.groupby('validator').agg(
        total_bytes_received=('msg.bytes', 'sum'),
        start_time_received=('msg.time', 'min'),
        end_time_received=('msg.time', 'max')
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

def plot_speeds(total_speeds):
    # Melt data for plotting
    plot_data = total_speeds.melt(id_vars=['validator'], value_vars=['upload_speed_mbps', 'download_speed_mbps'],
                                  var_name='Speed Type', value_name='Speed (Mbps)')

    # Create bar plot
    plt.figure(figsize=(12, 6))
    sns.barplot(data=plot_data, x='validator', y='Speed (Mbps)', hue='Speed Type')
    plt.title('Upload vs Download Speeds for Each Validator')
    plt.xlabel('Validator')
    plt.ylabel('Speed (Mbps)')
    plt.xticks(rotation=45)
    plt.legend(title='Speed Type')
    plt.show()

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

def plot_speeds_mbps(speeds_df):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=speeds_df, x='msg.time', y='speed_mbps', hue='action', estimator='mean')
    plt.title('Speed of Send/Receive Over Time')
    plt.xlabel('Time')
    plt.ylabel('Speed (Mbps)')
    plt.legend(title='Action')
    plt.show()


def plot_speed_between_regions_mbps(speeds_df, validators_df):
    # Merge speeds_df with validators_df to get regions
    speeds_df = speeds_df.merge(validators_df[['Public IPv4', 'Region']], left_on='validator', right_on='Public IPv4',
                                how='left')
    speeds_df = speeds_df.rename(columns={'Region': 'validator_region'})

    # Similarly, merge to get peer regions
    speeds_df = speeds_df.merge(validators_df[['Public IPv4', 'Region']], left_on='msg.peer_id', right_on='Public IPv4',
                                how='left', suffixes=('', '_peer'))
    speeds_df = speeds_df.rename(columns={'Region_peer': 'peer_region'})

    # Drop rows with missing regions
    speeds_df = speeds_df.dropna(subset=['validator_region', 'peer_region'])

    # Create a new column for region pair
    speeds_df['region_pair'] = speeds_df['validator_region'] + ' -> ' + speeds_df['peer_region']

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=speeds_df, x='msg.time', y='speed_mbps', hue='region_pair', estimator='mean')
    plt.title('Speed Between Regions Over Time')
    plt.xlabel('Time')
    plt.ylabel('Speed (Mbps)')
    plt.legend(title='Region Pair')
    plt.show()


def resample_speeds(speeds_df, interval='1T'):
    # Ensure 'msg.time' is a datetime object and set it as the index
    speeds_df = speeds_df.set_index('msg.time')

    # Resample the data and compute the mean
    resampled_df = speeds_df.resample(interval).mean().reset_index()

    return resampled_df


def plot_resampled_speeds_mbps(speeds_df):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=speeds_df, x='msg.time', y='speed_mbps', hue='action')
    plt.title('Average Speed of Send/Receive Over Time (Resampled)')
    plt.xlabel('Time')
    plt.ylabel('Speed (Mbps)')
    plt.legend(title='Action')
    plt.show()