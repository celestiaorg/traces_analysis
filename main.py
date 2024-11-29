# Press the green button in the gutter to run the script.
import pandas as pd

from compute_speed import compute_speeds, resample_speeds, plot_speeds_mbps, plot_speed_between_regions_mbps, \
    plot_resampled_speeds_mbps, calculate_total_speeds, plot_speeds
from parse_jsonl import process_experiment_data
from parse_validators_regions import parse_list_with_regions

if __name__ == '__main__':
    validators_df = parse_list_with_regions('list_with_regions.txt')

    print("finished parsing list with regions")
    # experiment_path = './traces_quic_900mb_buffer'
    experiment_path = './sample_data'
    received_df, sent_df, peers_df = process_experiment_data(experiment_path)
    print("finished processing experiment data")
    total_speeds = calculate_total_speeds(sent_df, received_df)
    print("finished calculating speeds")
    plot_speeds(total_speeds)

    print("done")