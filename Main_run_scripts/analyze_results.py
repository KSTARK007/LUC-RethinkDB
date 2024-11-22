#!/usr/bin/env python3

import os
import argparse
import re
import sys

def main():
    parser = argparse.ArgumentParser(description="Analyze results and generate metrics.")
    parser.add_argument('--io_thread_dir', required=True, help='IO Thread directory (e.g., io_unlimited, io_1)')
    parser.add_argument('--workload_type', required=True, help='Workload type (e.g., hotspot, uniform, zipfian)')
    parser.add_argument('--cache_size', required=True, help='Cache size')

    args = parser.parse_args()

    IO_THREAD_DIR = args.io_thread_dir
    WORKLOAD_TYPE = args.workload_type
    CACHE_SIZE = args.cache_size

    print(f"Analyzing results for cache size {CACHE_SIZE}, workload {WORKLOAD_TYPE}, and IO Thread {IO_THREAD_DIR}...")

    total_throughput = 0.0
    total_latency = 0.0
    client_count = 0

    result_dir = os.path.join('results', IO_THREAD_DIR, WORKLOAD_TYPE, f'cache_size_{CACHE_SIZE}')

    # Get list of client nodes
    client_node_dirs = [d for d in os.listdir(result_dir) if d.startswith('node_')]

    for node_dir in client_node_dirs:
        result_file = os.path.join(result_dir, node_dir, 'results.txt')
        if os.path.isfile(result_file):
            with open(result_file, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    throughput = float(lines[0].strip())
                    latency = float(lines[1].strip())
                    total_throughput += throughput
                    total_latency += latency
                    client_count += 1
                else:
                    print(f"Error: results.txt for {node_dir} does not have enough lines")
                    sys.exit(1)
        else:
            print(f"Error: Missing results.txt for {node_dir}")
            sys.exit(1)

    if client_count > 0:
        avg_latency = total_latency / client_count
    else:
        print("Error: No client results found!")
        sys.exit(1)

    # Analyze server logs
    total_rdma_hits = 0
    total_rdma_misses = 0

    server_node_dirs = [d for d in os.listdir(result_dir) if d.startswith('server_')]

    for node_dir in server_node_dirs:
        log_file = os.path.join(result_dir, node_dir, 'server_logs')
        if os.path.isfile(log_file):
            with open(log_file, 'r') as f:
                rdma_line = ''
                for line in f:
                    if 'RDMA hits:' in line:
                        rdma_line = line.strip()
                if rdma_line:
                    match = re.search(r'RDMA hits: (\d+)\s+Miss rate: (\d+)', rdma_line)
                    if match:
                        hits = int(match.group(1))
                        misses = int(match.group(2))
                        total_rdma_hits += hits
                        total_rdma_misses += misses
                    else:
                        print(f"Error: RDMA stats missing in server log for {node_dir}")
                        sys.exit(1)
                else:
                    print(f"Error: RDMA stats missing in server log for {node_dir}")
                    sys.exit(1)
        else:
            print(f"Error: Missing server_logs for {node_dir}")
            sys.exit(1)
    total_rdma_misses = total_rdma_misses  - 232575

    # Read current_pages_output files and extract keys
    integer_sets = []

    for node_dir in server_node_dirs:
        server_dir = os.path.join(result_dir, node_dir)
        current_pages_files = [f for f in os.listdir(server_dir) if f.startswith('current_pages_output') and f.endswith('.txt')]
        if current_pages_files:
            latest_file = max(current_pages_files, key=lambda f: int(re.findall(r'\d+', f)[0]) if re.findall(r'\d+', f) else 0)
            latest_file_path = os.path.join(server_dir, latest_file)
            with open(latest_file_path, 'r') as f:
                keys = set(map(int, f.read().split()))
                integer_sets.append(keys)
        else:
            print(f"Error: No current_pages_output file found for {node_dir}")
            sys.exit(1)

    # Compute Similarity and Sorensen similarity
    if integer_sets:
        # Compute union and intersection
        keys_union = set().union(*integer_sets)
        keys_intersect = set.intersection(*integer_sets)
        total_size = sum(len(s) for s in integer_sets)
        num_union = len(keys_union)
        num_intersect = len(keys_intersect)
        similarity = num_intersect / num_union if num_union > 0 else 0

        # Compute Sorensen similarity
        nsets = len(integer_sets)
        if nsets == 1:
            scale = 1
        else:
            scale = nsets / (nsets - 1)
        unique_per_total = num_union / total_size if total_size > 0 else 0
        if unique_per_total == 1:
            sorensen_similarity = 1
        else:
            sorensen_similarity = scale * (1 - (num_union / total_size))
    else:
        print("Error: No integer sets found for similarity calculations.")
        sys.exit(1)

    # Output results
    csv_file = os.path.join('results', IO_THREAD_DIR, WORKLOAD_TYPE, 'analysis.csv')
    csv_exists = os.path.isfile(csv_file)

    with open(csv_file, 'a') as f:
        if not csv_exists:
            f.write("Cache Size,Total Throughput,Avg Latency,RDMA Hits,Misses,Similarity,Sorensen Similarity\n")
        f.write(f"{CACHE_SIZE},{total_throughput},{avg_latency},{total_rdma_hits},{total_rdma_misses},{similarity},{sorensen_similarity}\n")

    print(f"Results for Cache Size {CACHE_SIZE}, Workload {WORKLOAD_TYPE}, and IO Thread {IO_THREAD_DIR}:")
    print(f"  Total Throughput: {total_throughput}")
    print(f"  Avg Latency: {avg_latency}")
    print(f"  RDMA Hits: {total_rdma_hits}")
    print(f"  RDMA Misses: {total_rdma_misses}")
    print(f"  Similarity: {similarity}")
    print(f"  Sorensen Similarity: {sorensen_similarity}")

if __name__ == "__main__":
    main()