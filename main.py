# Press the green button in the gutter to run the script.
import pandas as pd

from compute_speed import compute_speeds, resample_speeds, calculate_total_speeds, plot_speeds, calculate_total_regions_speeds, plot_region_speeds, \
    plot_speed_progression, plot_speed_progression_per_peer, calculate_speed_progression_per_peer
from parse_jsonl import process_experiment_data
from parse_validators_regions import parse_list_with_regions, parse_ip_to_region

# plot all
if __name__ == '__main__':
    # experiment_path = 'sample_data'
    # experiment_path = 'sample2'
    # experiment_path = 'sample3'
    # experiment_path = 'quic_0.5mb_32streams'
    # experiment_path = 'quic_0.5mb_1stream'
    # experiment_path = 'tm_native_no_bbr'
    # experiment_path = 'traces_tm_native_reno'
    # experiment_path = 'traces_quic_increasing_number_of_streams'
    # experiment_path = 'traces_quic_bench_4streams_5mb'
    # experiment_path = 'traces_tm_native_bbr_500bytes'
    # experiment_path = 'traces_tm_native_bbr_2mb'
    # experiment_path = 'traces_tm_native_bbr_5mb'
    experiment_path = 'traces_tm_native_long_running_10mb_intervals'
    # experiment_path = 'traces_tm_native_bbr_52_validators'
    # experiment_path = 'traces_quic_52_validators'
    # experiment_path = 'traces_bench_tool'

    # ips_to_regions = parse_ip_to_region('list_with_regions.txt')
    # ips_to_regions = parse_ip_to_region('quic_tool_list_with_regions.txt')
    ips_to_regions = parse_ip_to_region('list.txt')
    print("finished parsing list with regions")

    received_df, sent_df = process_experiment_data(experiment_path, ips_to_regions)
    print("finished processing experiment data")

    # plot each peer speed progression to all other peers combined
    plot_speed_progression(sent_df, received_df, "plots/"+experiment_path+"/")
    print("finished plotting speed progress")

    # plot the speed per region
    total_speeds = calculate_total_regions_speeds(sent_df, received_df)
    print("finished calculating region speeds")
    plot_region_speeds(total_speeds, "plots/"+experiment_path+"/")

    # plot average validator speeds
    total_speeds = calculate_total_speeds(sent_df, received_df)
    print("finished calculating total speeds for validators")
    plot_speeds(total_speeds, "plots/"+experiment_path+"/")

    # plot speed progression per peer to each target peer
    download_speed_data = calculate_speed_progression_per_peer(received_df, interval='10S')
    print("finished calculating download speeds")
    plot_speed_progression_per_peer(download_speed_data, ips_to_regions, "plots/"+experiment_path+"/download_speeds/")

    upload_speed_data = calculate_speed_progression_per_peer(sent_df, interval='10S')
    print("finished calculating upload speeds")
    plot_speed_progression_per_peer(upload_speed_data, ips_to_regions, "plots/"+experiment_path+"/upload_speeds/")

    print("done")

# this plots the progression of speed for each peer to its target peers
# if __name__ == '__main__':
#     # validators_df = parse_list_with_regions('list_with_regions.txt')
#     ips_to_regions = parse_ip_to_region('list_with_regions.txt')
#
#     print("finished parsing list with regions")
#     experiment_path = 'sample_data'
#     # experiment_path = 'traces_tm_native_bbr_52_validators'
#     received_df, sent_df, peers_df = process_experiment_data(experiment_path, ips_to_regions)
#     print("finished processing experiment data")
#     download_speed_data = calculate_speed_progression_per_peer(received_df, interval='10S')
#     print("finished calculating speeds")
#     plot_speed_progression_per_peer(download_speed_data, "plots/"+experiment_path+"/download_speeds/")
#     print("done download")
#
#     upload_speed_data = calculate_speed_progression_per_peer(sent_df, interval='10S')
#     print("finished calculating speeds")
#     plot_speed_progression_per_peer(upload_speed_data, "plots/"+experiment_path+"/upload_speeds/")
#     print("done upload")

# this plots the progression of download speed for each peer to its target peers
# if __name__ == '__main__':
#     # validators_df = parse_list_with_regions('list_with_regions.txt')
#     ips_to_regions = parse_ip_to_region('list_with_regions.txt')
#
#     print("finished parsing list with regions")
#     experiment_path = './sample_data'
#     received_df, sent_df, peers_df = process_experiment_data(experiment_path, ips_to_regions)
#     print("finished processing experiment data")
#     resampled_speeds = calculate_speed_per_peer(sent_df, ips_to_regions)
#     print("finished calculating speeds")
#     plot_speed_per_peer(resampled_speeds)
#     # plot_speeds(total_speeds)
#
#     print("done")

# this plots the speed progression
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
#     plot_speed_progression(sent_df, received_df)
#     print("finished calculating speeds")
#     # plot_speeds(total_speeds)
#
#     print("done")

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