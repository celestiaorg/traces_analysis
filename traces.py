import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def read_jsonl(root, chainID, nodeID, table):
    """
    Reads a JSONL file and returns a DataFrame.

    Args:
    root (str): The root path where the JSONL file is located.
    chainID (str): The chain ID.
    nodeID (str): The node ID.
    table (str): The table name.
    """
    path = os.path.join(root, chainID, nodeID, table + ".jsonl")

    try:
        df = pd.read_json(path, lines=True)
    except ValueError as e:
        # This handles cases like an empty file or improper JSON formatting
        print(f"Failed to read data from {path}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    return df.join(pd.json_normalize(df['msg'])).drop(columns=['msg'])


def list_directories(path):
    """
    Returns a list of directories found in the specified path.

    Args:
    path (str): The path where to look for directories.

    Returns:
    list: A list of directories found in the specified path.
    """
    if not os.path.isdir(path):
        return NotADirectoryError("The specified path is not a directory.")

    directories = []

    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        # Check if the entry is a directory
        if os.path.isdir(full_path):
            directories.append(entry)

    return directories


root = "/home/evan/traces/quic"
chainID = "1"
path = os.path.join(root, chainID)
node_ids = list_node_id_directories(path)
df = read_jsonl(root, chainID, "validator-0", "consensus_block")

print(df.info())