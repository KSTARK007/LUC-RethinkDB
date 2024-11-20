import rethinkdb as r
import threading
import random
import time
import argparse
from tqdm import tqdm


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
batch_size = 10000  # Batch size for inserts
operations_cap = 2000000  # Total read operations to perform
threads_per_node = 1  # Number of threads per node
VALUE_SIZE = 100
READ_BATCH_SIZE = 500


# Connect to RethinkDB per thread
def connect_to_rethinkdb_per_thread(host='10.10.1.1', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        connection.use(db)
        return connection, rdb
    except r.errors.RqlDriverError as e:
        print(f"Failed to connect to RethinkDB on {host}: {e}")
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
    finally:
        connection.close()


# Batch insert data
def insert_data():
    connection, rdb = connect_to_rethinkdb_per_thread(host=hosts[0])
    if not connection:
        return
    try:
        print(f"Inserting {data_size} records into table '{table_name}' in batches of {batch_size}...")
        for i in range(0, data_size, batch_size):
            batch = [{"id": j, "value": f"value_{j}".ljust(VALUE_SIZE, '0')} for j in range(i + 1, min(i + batch_size + 1, data_size + 1))]
            rdb.table(table_name).insert(batch).run(connection)
            print(f"Inserted batch {i // batch_size + 1}")
        print("Data insertion completed.")
    finally:
        connection.close()


# Verify data correctness
def verify_data(key, value):
    expected_value = f"value_{key}".ljust(VALUE_SIZE, '0')
    return value == expected_value


# Read data in batches
def batch_read_data_from_node(node_id, thread_id, start, end, batch_size, operations_per_thread, thread_metrics):
    host = node_to_host[node_id]
    connection, rdb = connect_to_rethinkdb_per_thread(host=host)
    if not connection:
        return
    total_latency = 0
    completed_operations = 0
    start_time = time.time()

    try:
        print(f"Node {node_id}, Thread {thread_id} reading data in batches of size {batch_size} from {host}...")
        for _ in range(0, operations_per_thread, batch_size):
            batch_keys = [random.randint(start, end) for _ in range(batch_size)]
            op_start_time = time.time()

            # Batch read request
            results = rdb.table(table_name).get_all(*batch_keys).run(connection, read_mode='outdated')

            # Calculate latency
            op_latency = time.time() - op_start_time
            total_latency += op_latency
            completed_operations += len(batch_keys)

            # Validate results (optional)
            for key in batch_keys:
                record = next(results, None)
                if record:
                    verify_data(key, record['value'])

            # Store metrics
            thread_metrics["latency"].append(op_latency)

            # Print intermediate stats every 10,000 operations
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
def start_read_threads(start, end, batch_size):
    threads = []
    all_thread_metrics = []

    # Calculate operations per thread
    total_threads = len(node_to_host) * threads_per_node
    operations_per_thread = operations_cap // total_threads

    # Start threads for each node
    for node_id in node_to_host:
        for thread_id in range(threads_per_node):
            thread_metrics = {"latency": [], "throughput": 0}
            t = threading.Thread(
                target=batch_read_data_from_node,
                args=(node_id, thread_id + 1, start, end, batch_size, operations_per_thread, thread_metrics)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RethinkDB batch read script.")
    parser.add_argument("--leader", action="store_true", help="Enable leader mode to load data before running requests.")
    args = parser.parse_args()

    setup_database()

    # Leader mode: Insert data
    if args.leader:
        insert_data()
    else:
        start_read_threads(1, data_size, READ_BATCH_SIZE)
