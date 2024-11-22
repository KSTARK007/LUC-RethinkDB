#!/bin/bash

# Get parameters
CACHE_PERCENTAGE=$1
IO_THREAD=$2
WORKLOAD_TYPE=$3

# Define global variables
SERVER_NODES=("10.10.1.1" "10.10.1.2" "10.10.1.3")
CLIENT_NODES=("10.10.1.4" "10.10.1.5" "10.10.1.6" "10.10.1.7" "10.10.1.8" "10.10.1.9")
CONTROL_NODE="10.10.1.4"
MAX_CACHE=345

# Function to clean up before starting a new iteration
cleanup() {
  echo "Cleaning up logs and results on nodes..."
  for node in "${SERVER_NODES[@]}" "${CLIENT_NODES[@]}"; do
    ssh "$node" "cd /mydata && rm -f server_logs results.txt read_logs.txt writes.txt current_pages_output* page_map_output* remote_page_out*"
  done
}

# Function to start servers in parallel with io-threads
start_servers() {
  echo "Starting servers in parallel with cache size $CACHE_SIZE and io-threads set to 1..."
  for node in "${SERVER_NODES[@]}"; do
    ssh "$node" "cd /mydata && \
      pkill -9 rethinkdb; \
      rm -rf current_pages_output* page_map_output* rethinkdb_data/ remote_page_out* server_logs; \
      /proj/rasl-PG0/kiran/rethink-source/RethinkDB/build/release/rethinkdb \
        --bind all --direct-io --join 10.10.1.1:29015 \
        --io-threads 1 --cache-size $CACHE_SIZE \
        > server_logs 2>&1 &"
    echo "Server started on $node"
  done
}

# Function to start servers in parallel without specifying io-threads
start_servers_without_io_threads() {
  echo "Starting servers in parallel with cache size $CACHE_SIZE and unlimited io-threads..."
  for node in "${SERVER_NODES[@]}"; do
    ssh "$node" "cd /mydata && \
      pkill -9 rethinkdb; \
      rm -rf current_pages_output* page_map_output* rethinkdb_data/ remote_page_out* server_logs; \
      /proj/rasl-PG0/kiran/rethink-source/RethinkDB/build/release/rethinkdb \
        --bind all --direct-io --join 10.10.1.1:29015 \
        --cache-size $CACHE_SIZE \
        > server_logs 2>&1 &"
    echo "Server started on $node"
  done
}

# Function to check server logs for readiness
check_server_ready() {
  echo "Checking server readiness..."
  while true; do
    all_ready=true
    for node in "${SERVER_NODES[@]}"; do
      ssh "$node" "grep -q 'Server ready,' /mydata/server_logs"
      if [ $? -ne 0 ]; then
        all_ready=false
        break
      fi
    done
    if $all_ready; then
      echo "All servers are ready."
      break
    else
      echo "Waiting for servers to be ready..."
      sleep 2
    fi
  done
}

# Function to run the leader operation on the control node
run_leader_operation() {
  echo "Running leader operation on control node..."
  ssh "$CONTROL_NODE" "cd /mydata && rm -f writes.txt && python basic.py --leader > writes.txt 2>&1"
}

# Function to issue read operations on all client nodes and the control node
issue_read_operations() {
  WORKLOAD_TYPE=$1
  echo "Issuing read operations on client nodes and control node in parallel..."
  for i in "${!CLIENT_NODES[@]}"; do
    node="${CLIENT_NODES[$i]}"
    NODE_NUMBER=$i
    ssh -n -f "$node" "cd /mydata && rm -f results.txt read_logs.txt && nohup python basic.py --workload ${WORKLOAD_TYPE} --node ${NODE_NUMBER} > read_logs.txt 2>&1 &"
  done

  # Wait until results.txt exists on all client nodes
  echo "Waiting for read operations to complete on all client nodes..."
  sleep 180  # Wait for 3 minutes before checking
  while true; do
    all_done=true
    for node in "${CLIENT_NODES[@]}"; do
      ssh "$node" "test -f /mydata/results.txt"
      if [ $? -ne 0 ]; then
        all_done=false
        break
      fi
    done
    if $all_done; then
      echo "All client nodes have completed read operations."
      break
    else
      echo "Waiting for clients to complete..."
      sleep 5
    fi
  done
}

# Function to collect results
collect_results() {
  echo "Collecting results for cache percentage $CACHE_PERCENTAGE, workload $WORKLOAD_TYPE, and IO Thread $IO_THREAD..."
  RESULT_DIR="results/$IO_THREAD_DIR/$WORKLOAD_TYPE/cache_size_$CACHE_PERCENTAGE"
  mkdir -p "$RESULT_DIR"

  # Collect client results
  for i in "${!CLIENT_NODES[@]}"; do
    node="${CLIENT_NODES[$i]}"
    NODE_DIR="$RESULT_DIR/node_${i}"
    mkdir -p "$NODE_DIR"
    scp "$node:/mydata/results.txt" "$NODE_DIR/results.txt"
    scp "$node:/mydata/read_logs.txt" "$NODE_DIR/read_logs.txt"
  done

  # Collect server logs and current_pages_output files
  echo "Collecting server logs and current_pages_output files..."
  for i in "${!SERVER_NODES[@]}"; do
    node="${SERVER_NODES[$i]}"
    SERVER_DIR="$RESULT_DIR/server_${i}"
    mkdir -p "$SERVER_DIR"

    # Copy server_logs
    scp "$node:/mydata/server_logs" "$SERVER_DIR/server_logs"

    # Find the highest numbered current_pages_output file on the server node
    latest_file=$(ssh "$node" "cd /mydata && ls -1 current_pages_output*.txt 2>/dev/null | sort -V | tail -n 1")
    if [ -n "$latest_file" ]; then
      scp "$node:/mydata/$latest_file" "$SERVER_DIR/$latest_file"
    else
      echo "Warning: No current_pages_output files found on server_${i}"
    fi
  done
}

# Set IO_THREAD_DIR for output paths
if [ "$IO_THREAD" == "unlimited" ]; then
  IO_THREAD_DIR="io_unlimited"
else
  IO_THREAD_DIR="io_$IO_THREAD"
fi

# Calculate CACHE_SIZE
CACHE_SIZE=$(awk "BEGIN {print int(($MAX_CACHE * $CACHE_PERCENTAGE / 100) + 0.5)}")
echo "Starting iteration for cache percentage: ${CACHE_PERCENTAGE}% (Cache Size: $CACHE_SIZE)"

# Run the iteration
cleanup
if [ "$IO_THREAD" == "unlimited" ]; then
  start_servers_without_io_threads
else
  start_servers
fi
sleep 10  # Give servers time to start
check_server_ready
run_leader_operation
issue_read_operations "$WORKLOAD_TYPE"
collect_results

# Call the Python script for analysis
python3 analyze_results.py --io_thread_dir "$IO_THREAD_DIR" --workload_type "$WORKLOAD_TYPE" --cache_size "$CACHE_PERCENTAGE"
