#!/bin/bash

# IPs of the nodes
NODE1_IP="10.10.1.1"
NODE2_IP="10.10.1.2"
NODE3_IP="10.10.1.3"

# List of client nodes
CLIENTS=("10.10.1.1" "10.10.1.2" "10.10.1.3") # Add the actual client IPs here

# CSV output file
CSV_OUTPUT_FILE="performance_metrics.csv"

RETHINK_BIN_PATH="/proj/rasl-PG0/kiran/rethink-source/rethinkdb/build/release/rethinkdb"

# Initialize the CSV file with headers
echo "Workload,Cache Size,Direct IO,Total Time Taken (seconds),Average Throughput (records/sec)" > $CSV_OUTPUT_FILE

# Function to start rethinkdb on all nodes
start_rethinkdb() {
    local cache_size=$1
    local direct_io=$2

    echo "Starting rethinkdb with cache size: $cache_size MB and direct-io: $direct_io"

    # Start rethinkdb on Node 1
    # ssh $NODE1_IP "$RETHINK_BIN_PATH --bind all --cache-size $cache_size $direct_io &> /mydata/logs & echo \$! > /tmp/rethinkdb_node1.pid"
    echo "$RETHINK_BIN_PATH --bind all --cache-size $cache_size $direct_io &> /mydata/logs & echo \$! > /tmp/rethinkdb_node1.pid"
    
    # Start rethinkdb on Node 2
    # ssh $NODE2_IP "$RETHINK_BIN_PATH --join $NODE1_IP:29015 --bind all --cache-size $cache_size $direct_io &> /mydata/logs & echo \$! > /tmp/rethinkdb_node2.pid"
    echo "$RETHINK_BIN_PATH --join $NODE1_IP:29015 --bind all --cache-size $cache_size $direct_io &> /mydata/logs & echo \$! > /tmp/rethinkdb_node2.pid"
    
    # Start rethinkdb on Node 3
    # ssh $NODE3_IP "$RETHINK_BIN_PATH --join $NODE1_IP:29015 --bind all --cache-size $cache_size $direct_io &> /mydata/logs & echo \$! > /tmp/rethinkdb_node3.pid"
    
    echo "RethinkDB started on all nodes"
}

# Function to run the Python script on all clients for a given workload, batch size, and total records in parallel
# Function to run the Python script on all clients for a given workload, batch size, and total records in parallel
run_python_script() {
    local workload=$1
    local batch_size=$2
    local total_records=$3
    local cache_size=$4
    local direct_io=$5

    echo "Running Python script for workload: $workload with batch size: $batch_size and total records: $total_records on all clients"

    # Initialize aggregation variables
    local total_time_taken=0
    local total_throughput=0
    local client_count=0
    local pids=()
    
    # Track whether this is the first client
    local first_client=true

    # Run the script in parallel on all clients
    for client in "${CLIENTS[@]}"; do
        echo "Running on client: $client"

        # Check if this is the first client to add the --leader flag
        if [ "$first_client" = true ]; then
            ssh $client "cd /mydata; python3 main_driver.py --workload $workload --batch_size $batch_size --total_records $total_records --leader > /tmp/main_driver_output_${workload}_${client}.txt" &
            first_client=false  # Mark the flag as set for the first client
        else
            ssh $client "cd /mydata; python3 main_driver.py --workload $workload --batch_size $batch_size --total_records $total_records > /tmp/main_driver_output_${workload}_${client}.txt" &
        fi

        pids+=($!)  # Store the PID of the background process
    done

    echo "Python script running on all clients in parallel..."

    # Wait for all parallel processes to finish
    for pid in "${pids[@]}"; do
        wait $pid
    done

    echo "Python script execution completed on all clients."

    # Fetch and aggregate results
    for client in "${CLIENTS[@]}"; do
        # Fetch the output file from the client
        scp $client:/tmp/main_driver_output_${workload}_${client}.txt ./main_driver_output_${workload}_${client}.txt

        # Extract the time taken and throughput from the Python script output
        local time_taken=$(grep 'Total time taken:' ./main_driver_output_${workload}_${client}.txt | awk '{print $4}')
        local throughput=$(grep 'Throughput:' ./main_driver_output_${workload}_${client}.txt | awk '{print $2}')

        # Sum up the time taken and throughput for aggregation
        total_time_taken=$(echo "$total_time_taken + $time_taken" | bc)
        total_throughput=$(echo "$total_throughput + $throughput" | bc)
        client_count=$((client_count + 1))
    done

    # Calculate the average throughput
    local average_throughput=$(echo "scale=2; $total_throughput / $client_count" | bc)

    # Append aggregated results to the CSV file
    echo "$workload,$cache_size,$direct_io,$total_time_taken,$average_throughput" >> $CSV_OUTPUT_FILE

    echo "Results aggregated and appended to CSV."
}


# Function to kill rethinkdb processes
kill_rethinkdb() {
    echo "Killing rethinkdb processes on all nodes..."
    local pids=(
        $(ssh $NODE1_IP "cat /tmp/rethinkdb_node1.pid")
        $(ssh $NODE2_IP "cat /tmp/rethinkdb_node2.pid")
        $(ssh $NODE3_IP "cat /tmp/rethinkdb_node3.pid")
    )
    ssh $NODE1_IP "kill -9 ${pids[0]}"
    ssh $NODE2_IP "kill -9 ${pids[1]}"
    ssh $NODE3_IP "kill -9 ${pids[2]}"
    echo "Rethinkdb processes killed."
}

# Function to clear buffer cache on all nodes
clear_cache() {
    echo "Clearing buffer and page cache on all nodes..."
    ssh $NODE1_IP "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
    ssh $NODE2_IP "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
    ssh $NODE3_IP "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
    echo "Cache cleared on all nodes."
}

# Main function that iterates through the cache sizes, direct-io options, and workloads
iterate_cache_sizes_and_workloads() {
    # local cache_sizes=(5 50 125 250 375 500 550)
    # local direct_io_options=("" "--direct-io")
    # local workloads=("zipfian" "uniform" "hotspot")
    
    local cache_sizes=(550)
    # local direct_io_options=("--direct-io")
    local direct_io_options=("")
    local workloads=("uniform")

    # Define batch size and total records for all workloads
    local batch_size=10000
    local total_records=5000000

    for cache_size in "${cache_sizes[@]}"; do
        for direct_io in "${direct_io_options[@]}"; do
            start_rethinkdb $cache_size "$direct_io"

            # Iterate over the workloads and run Python for each workload
            for workload in "${workloads[@]}"; do
                echo "Running workload $workload with cache size $cache_size and direct_io $direct_io"
                run_python_script $workload $batch_size $total_records $cache_size "$direct_io"
            done

            kill_rethinkdb
            clear_cache
        done
    done
}


iterate_cache_sizes_and_workloads