# Press the green button in the gutter to run the script.
import pandas as pd

from compute_speed import compute_speeds, resample_speeds, plot_speeds_mbps, plot_speed_between_regions_mbps, \
    plot_resampled_speeds_mbps, calculate_total_speeds, plot_speeds, calculate_total_regions_speeds, plot_region_speeds
from parse_jsonl import process_experiment_data
from parse_validators_regions import parse_list_with_regions, parse_ip_to_region

if __name__ == '__main__':
    # validators_df = parse_list_with_regions('list_with_regions.txt')
    ips_to_regions = parse_ip_to_region('list_with_regions.txt')

    print("finished parsing list with regions")
    experiment_path = './traces_quic_with_logs_and_udp_optimisations'
    # experiment_path = './sample_data'
    # experiment_path = './traces_tm_native_with_bbr'
    received_df, sent_df, peers_df = process_experiment_data(experiment_path, ips_to_regions)
    print("finished processing experiment data")
    total_speeds = calculate_total_regions_speeds(sent_df, received_df)
    print("finished calculating speeds")
    plot_region_speeds(total_speeds)

    print("done")

# this plots the speed per region
# if __name__ == '__main__':
#     # validators_df = parse_list_with_regions('list_with_regions.txt')
#     ips_to_regions = parse_ip_to_region('list_with_regions.txt')
#
#     print("finished parsing list with regions")
#     experiment_path = './traces_quic_with_logs_and_udp_optimisations'
#     # experiment_path = './sample_data'
#     # experiment_path = './traces_tm_native_with_bbr'
#     received_df, sent_df, peers_df = process_experiment_data(experiment_path, ips_to_regions)
#     print("finished processing experiment data")
#     total_speeds = calculate_total_regions_speeds(sent_df, received_df)
#     print("finished calculating speeds")
#     plot_region_speeds(total_speeds)
#
#     print("done")

# this plots the validator speeds along with their regions
# if __name__ == '__main__':
#     # validators_df = parse_list_with_regions('list_with_regions.txt')
#     ips_to_regions = parse_ip_to_region('list_with_regions.txt')
#
#     print("finished parsing list with regions")
#     experiment_path = './traces_quic_with_logs_and_udp_optimisations'
#     # experiment_path = './sample_data'
#     # experiment_path = './traces_tm_native_with_bbr'
#     received_df, sent_df, peers_df = process_experiment_data(experiment_path, ips_to_regions)
#     print("finished processing experiment data")
#     total_speeds = calculate_total_speeds(sent_df, received_df)
#     print("finished calculating speeds")
#     plot_speeds(total_speeds)
#
#     print("done")