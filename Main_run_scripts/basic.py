import rethinkdb as r
import threading
import time
import argparse
import os
from itertools import cycle
import random

type_random = True
type_soft = True
# Configuration
hosts = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
node_to_host = {
    1: "10.10.1.1",
    2: "10.10.1.2",
    3: "10.10.1.3",
}
db_name = 'test-rac'
table_name = 'my_table'
data_size = 1000000  # Total records to insert
# data_size = 100000  # Total records to insert
batch_size = 10000  # Batch size for inserts
operations_cap = 200000  # Total read operations to perform
threads_per_node = 1  # Number of threads per node
VALUE_SIZE = 100
READ_BATCH_SIZE = 500
READ_DURATION = 2 * 60  # Default 4 minutes in seconds

# Helper: Connect to RethinkDB
def connect_to_rethinkdb_per_thread(host='10.10.1.1', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        connection.use(db)
        return connection, rdb
    except r.errors.RqlDriverError as e:
        print(f"Failed to connect to RethinkDB on {host}: {e}")
        return None, None

def connect_to_rethinkdb_for_writes_per_thread(db=db_name, table_name=table_name):
    try:
        # Initially connect to one of the hosts to retrieve cluster information
        initial_host = hosts[0]
        rdb = r.RethinkDB()
        connection = rdb.connect(host=initial_host, port=28015)
        connection.use(db)

        # Fetch the primary node for the specified table
        table_status = rdb.table(table_name).status().run(connection)
        primary_node = table_status['shards'][0]['primary_replicas'][0]  # Get the first primary node

        # Close the initial connection
        connection.close()

        node_number = primary_node.split("_")[0].split("e")[1]
        node_number = int(node_number) + 1
        node=f"10.10.1.{node_number}"
        print(f"Primary node for writes on table '{table_name}': {node}")

        # Establish a new connection to the primary node
        connection = rdb.connect(host=node, port=28015)
        connection.use(db)
        return connection, rdb
    except r.errors.RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None
    except Exception as e:
        print(f"Failed to determine the primary node: {e}")
        return None, None

# Setup database and table
def setup_database():
    connection, rdb = connect_to_rethinkdb_per_thread(host=hosts[0])
    if not connection:
        return
    try:
        # Create database if it doesn't exist
        if db_name not in rdb.db_list().run(connection):
            rdb.db_create(db_name).run(connection)
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

        # Create table if it doesn't exist
        if table_name not in rdb.db(db_name).table_list().run(connection):
            rdb.db(db_name).table_create(table_name, shards=1, replicas=3).run(connection)
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")
        if(not type_soft):
            rdb.db(db_name).table(table_name).config().update({
                'write_acks': 'majority',
                'durability': 'hard'
            }).run(connection)
        else:
            rdb.db(db_name).table(table_name).config().update({
                'write_acks': 'single',
                'durability': 'soft'
            }).run(connection)
    finally:
        connection.close()

# Batch insert data
def insert_data():
    connection, rdb = connect_to_rethinkdb_per_thread(host=hosts[0])
    if not connection:
        return
    try:
        print(f"Inserting {data_size} records into table '{table_name}' in batches of {batch_size}...")
        latency_buckets = []
        for i in range(0, data_size, batch_size):
            batch = [{"id": j, "value": f"value_{j}".ljust(VALUE_SIZE, '0')} for j in range(i + 1, min(i + batch_size + 1, data_size + 1))]
            start = time.time()
            rdb.table(table_name).insert(batch).run(connection)
            latency = time.time() - start
            latency_buckets.append(latency)
            print(f"Inserted batch {i // batch_size + 1}")
            print(f"Latency: {(latency/batch_size)*1000000} us")
            print(f"Throughput: {batch_size / latency} ops/sec")
        print("Data insertion completed.")
        print(f"Latency: {(sum(latency_buckets) / data_size) * 1000000} us")
        print(f"Throughput: {data_size / sum(latency_buckets)} ops/sec")
    finally:
        connection.close()

def insert_random_test():
    connection, rdb = connect_to_rethinkdb_per_thread(host=hosts[0])
    if not connection:
        return
    try:
        print(f"Inserting {data_size} records into table '{table_name}' in batches of {batch_size}...")

        indices = list(range(1, data_size + 1))
        random.shuffle(indices)

        # Insert data in random batches
        latency_buckets = []
        for i in range(0, data_size, batch_size):
            batch_indices = indices[i:i + batch_size]
            batch = [{"id": idx, "value": f"value_{idx}".ljust(VALUE_SIZE, '0')} for idx in batch_indices]
            start = time.time()
            rdb.table(table_name).insert(batch).run(connection)
            latency = time.time() - start
            latency_buckets.append(latency)
            print(f"Inserted batch {i // batch_size + 1}")
            print(f"Latency: {(latency/batch_size) * 1000000} us")
            print(f"Throughput: {batch_size / latency} ops/sec")
        print("Data insertion completed.")
        print(f"Latency: {(sum(latency_buckets) / data_size) * 1000000} us")
        print(f"Throughput: {data_size / sum(latency_buckets)} ops/sec")
    finally:
        connection.close()

# Helper: Load keys from workload files
def load_keys(workload_type, node_number, thread_id, ycsb_type):
    # folder_path = f"workloads/100k_10M/{ycsb_type}"
    folder_path = f"/mydata/workloads/1M/{ycsb_type}"
    files = sorted(
        [f for f in os.listdir(folder_path) if f.startswith(f"client_{node_number}_thread_")],
        key=lambda x: int(x.split('_thread_')[1].split('_')[0])
    )
    if not files:
        raise ValueError(f"No workload files found for workload type {workload_type} and node {node_number}.")

    # Cycle through files if there are fewer files than threads
    file_path = os.path.join(folder_path, files[thread_id % len(files)])
    keys = []
    key_number = data_size
    with open(file_path, 'r') as file:
        for line in file:
            key, node, op = line.split()
            keys.append((int(line.split()[0]), op))  # Take the first number in each line as the key
    return keys

# Batch read data for a fixed duration
def batch_read_data_from_node(node_id, thread_id, start, end, batch_size, thread_metrics, workload_type, node_number, duration=READ_DURATION, ycsb_type="a"):
    host = node_to_host[node_id]
    connection, rdb = connect_to_rethinkdb_per_thread(host=host)
    writes_connection, writes_rdb = connect_to_rethinkdb_per_thread(host='10.10.1.1')
    if not connection:
        return
    keys = load_keys(workload_type, node_number, thread_id - 1, ycsb_type)
    keys_cycle = cycle(keys)  # Repeat keys if necessary

    total_latency = 0
    completed_operations = 0
    start_time = time.time()

    try:
        print(f"Node {node_id}, Thread {thread_id} reading data in batches of size {batch_size} from {host}...")
        prev_key_op = None
        read_keys = []
        write_keys = []
        while time.time() - start_time < duration:
            batch_keys = []
            expect_read = None
            # for i in range(batch_size):
            while True:
                if len(read_keys) > batch_size:
                    expect_read = "R"
                    break
                if len(write_keys) > batch_size:
                    expect_read = "U"
                    break

                # key, op = prev_key_op
                key, op = next(keys_cycle)
                # if key >= data_size:
                #     continue

                if op == "R":
                    # batch_keys.append(key)
                    read_keys.append(key)
                    expect_read = "R"
                # elif op == "U" or op == "I":
                else:
                    # if expect_read is not None and expect_read != "U":
                    #     break
                    # batch_keys.append({"id": key, "value": f"value_{key}".ljust(VALUE_SIZE, '0')})
                    # print(f"Node {node_id}, Thread {thread_id} is writing {key}")
                    write_keys.append({"id": key, "value": f"value_{key}".ljust(VALUE_SIZE, '0')})
                    expect_read = "U"

            op_start_time = time.time()

            # Batch read request
            if expect_read == "R":
                batch_keys = read_keys
                results = rdb.table(table_name).get_all(*batch_keys).run(connection, read_mode='outdated')
                read_keys = []
            else:
                batch_keys = write_keys
                results = rdb.table(table_name).insert(batch_keys).run(connection, read_mode='outdated')
                # results = rdb.table(table_name).insert(batch_keys, durability='soft').run(connection, read_mode='outdated')
                # results = writes_rdb.table(table_name).insert(batch_keys).run(writes_connection, read_mode='outdated')
                write_keys = []

            # Calculate latency
            op_latency = time.time() - op_start_time
            total_latency += op_latency
            completed_operations += len(batch_keys)

            # Store metrics
            thread_metrics["latency"].append(op_latency)

            if completed_operations % 100000 == 0:
                elapsed_time = time.time() - start_time
                throughput = completed_operations / elapsed_time
                avg_latency = total_latency / completed_operations
                print(f"Node {node_id}, Thread {thread_id}: Completed {completed_operations} ops. "
                      f"Throughput: {throughput:.2f} ops/sec, Avg Latency: {avg_latency:.6f} sec")

        # Final stats for the thread
        total_time = time.time() - start_time
        thread_metrics["throughput"] = completed_operations / total_time if total_time > 0 else 0

    except KeyboardInterrupt:
        print(f"Stopping thread {thread_id} for node {node_id}.")
    finally:
        connection.close()

# Start read threads
def start_read_threads(workload_type, node_number, batch_size, duration=READ_DURATION, ycsb_type="a"):
    threads = []
    all_thread_metrics = []

    # Start threads for each node
    for node_id in node_to_host:
        for thread_id in range(threads_per_node):
            thread_metrics = {"latency": [], "throughput": 0}
            t = threading.Thread(
                target=batch_read_data_from_node,
                args=(node_id, thread_id + 1, 1, data_size, batch_size, thread_metrics, workload_type, node_number, duration, ycsb_type)
            )
            threads.append((t, thread_metrics))
            all_thread_metrics.append((node_id, thread_id + 1, thread_metrics))
            t.start()

    # Wait for all threads to finish
    for t, thread_metrics in threads:
        t.join()

    # Print stats per thread
    print("\nPer-Thread Results:")
    total_throughput = 0
    latency_cal = 0
    for node_id, thread_id, metrics in all_thread_metrics:
        total_ops = len(metrics["latency"]) * batch_size
        total_latency = sum(metrics["latency"])
        avg_latency = total_latency / total_ops if total_ops > 0 else 0
        throughput = metrics["throughput"]
        total_throughput += throughput
        latency_cal += avg_latency
        print(f"Node {node_id}, Thread {thread_id}: Total Ops: {total_ops}, "
              f"Avg Latency: {avg_latency:.6f} sec, Throughput: {throughput:.2f} ops/sec")
    avg_latency_t = latency_cal / len(all_thread_metrics)
    avg_latency_t = avg_latency_t * 1000000
    # Final combined results
    print(f"\nFinal Combined Results:")
    print(f"Total Throughput: {total_throughput:.2f} ops/sec")
    print(f"Average Latency: {avg_latency_t:.6f} us")

    # Write results to file
    file = open("results.txt", "w")
    file.write(f"{total_throughput:.2f}\n")
    file.write(f"{avg_latency_t:.6f}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RethinkDB batch read script.")
    parser.add_argument("--leader", action="store_true", help="Enable leader mode to load data before running requests.")
    parser.add_argument("--workload", type=str, required=False, help="Workload type: hotspot, uniform, or zipfian.")
    parser.add_argument("--ycsb", type=str, required=False, help="YCSB type: a, b, c, d , f")
    parser.add_argument("--node", type=int, required=False, help="Node number (0-indexed).")
    parser.add_argument("--soft", type=int, help="Enable soft durability.")
    parser.add_argument("--duration", type=int, default=READ_DURATION, help="Read duration in seconds (default: 240).")

    args = parser.parse_args()

    setup_database()
    type_random = False
    # type_random = True
    
    # type_soft = True
    type_soft = args.soft
    if(args.soft == 1):
        type_soft = True
    else:
        type_soft = False
    # Leader mode: Insert data
    if args.leader:
        # insert_data()
        # insert_random_test()
        if(not type_random):
            insert_data()
        else:
            insert_random_test()
    else:
        if not args.workload or args.node is None:
            print("Error: Workload type and node number must be specified for read operations.")
            exit(1)
        # print(type_soft, args.ycsb)
        start_read_threads(args.workload, args.node, READ_BATCH_SIZE, args.duration, args.ycsb)
