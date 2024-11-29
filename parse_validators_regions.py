import pandas as pd
import re

def parse_list_with_regions(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # Skip the header and empty lines
    data_lines = [line for line in lines if line.strip() and not line.startswith('ID')]

    # Extract columns using regex
    pattern = re.compile(r'\s*(\d+)\s+(\S+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(.*?)\s+([\w-]+)\s+(\w+)\s*(.*)')
    records = []
    for line in data_lines:
        match = pattern.match(line)
        if match:
            groups = match.groups()
            record = {
                'ID': groups[0],
                'Name': groups[1],
                'Public IPv4': groups[2],
                'Private IPv4': groups[3],
                'Public IPv6': groups[4],
                'Memory': groups[5],
                'VCPUs': groups[6],
                'Disk': groups[7],
                'Region': groups[8],
                'Image': groups[9],
                'VPC UUID': groups[10],
                'Status': groups[11],
                'Tags/Features/Volumes': groups[12],
            }
            records.append(record)
        else:
            print(f"Line skipped (could not parse): {line.strip()}")

    df = pd.DataFrame(records)
    return df
