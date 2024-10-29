import rethinkdb as r
import os
import json
from rethinkdb.errors import RqlRuntimeError, RqlDriverError
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
import random
from datetime import datetime
import time

VALUE_SIZE = 100
db_name = 'test-rac'
trigger_file = '/proj/rasl-PG0/kiran/rethink-source/trigger.txt'

def connect_to_rethinkdb_per_thread(host='localhost', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        connection.use(db)  # Switch to the appropriate database
        return connection, rdb
    except RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None

# Function to connect to RethinkDB instance and create the database if it doesn't exist
def connect_to_rethinkdb(host='localhost', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        print(f"Connected to RethinkDB on {host}:{port}")

        # Check if the database exists, if not create it
        if db not in rdb.db_list().run(connection):
            rdb.db_create(db).run(connection)
            print(f"Database '{db}' created.")
        else:
            print(f"Database '{db}' already exists.")
        
        connection.use(db)
        return connection, rdb
    except RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None

# Function to delete existing tables in the database
def delete_existing_tables(connection, rdb):
    try:
        tables = rdb.db(connection.db).table_list().run(connection)
        for table in tables:
            rdb.db(connection.db).table_drop(table).run(connection)
            print(f"Table '{table}' deleted.")
    except RqlRuntimeError as e:
        print(f"Error deleting tables: {e}")

# Function to create a table if it doesn't exist and set replication to 3
def create_table(connection, rdb, table_name):
    try:
        if table_name not in rdb.db(connection.db).table_list().run(connection):
            rdb.db(connection.db).table_create(table_name, shards=1, replicas=3).run(connection)
            # rdb.db(connection.db).table_create(table_name, shards=1, replicas=2).run(connection)
            print(f"Table '{table_name}' created with replication set to 3.")
        else:
            print(f"Table '{table_name}' already exists.")
    except RqlRuntimeError as e:
        print(f"Error creating table: {e}")

# Function to generate a database file with 1M key-value pairs if it doesn't exist
def generate_db_file(file_name='db_data.json', size=1000000):
    if not os.path.exists(file_name):
        print(f"Generating database file '{file_name}' with 1M key-value pairs...")
        # data = [{'id': i, 'value': f"value_{i}"} for i in range(size)]
        data = [{'id': i, 'value': f"value_{i}".ljust(VALUE_SIZE, '0')} for i in range(size)]
        with open(file_name, 'w') as f:
            json.dump(data, f)
        print(f"Database file '{file_name}' created.")
    else:
        print(f"Database file '{file_name}' already exists.")

def insert_batch(thread_id, table_name, batch, batch_num, host, port, db, chunk_size=1000):
    connection, rdb = connect_to_rethinkdb_per_thread(host, port, db)
    if connection is None:
        print(f"Thread {thread_id}: Failed to connect to RethinkDB")
        return
    
    try:
        # Break down the batch into smaller chunks
        for i in range(0, len(batch), chunk_size):
            sub_batch = batch[i:i + chunk_size]
            result = rdb.table(table_name).insert(sub_batch).run(connection)
            print(f"Thread {thread_id}: Inserted sub-batch {i // chunk_size + 1} - {len(sub_batch)} records into '{table_name}'.")

    except Exception as e:
        print(f"Thread {thread_id}: Error inserting batch {batch_num}: {e}")
    finally:
        connection.close()



# Function to read data from the database file and insert into the database in parallel
def insert_from_db_file(host='localhost', port=28015, db=db_name, table_name='my_table', file_name='db_data.json', batch_size=50000, num_threads=10):
    if os.path.exists(file_name):
        print(f"Reading data from '{file_name}' and inserting into the table '{table_name}' with {num_threads} threads...")
        with open(file_name, 'r') as f:
            data = json.load(f)

            # Use ThreadPoolExecutor for parallel inserts
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    executor.submit(insert_batch, batch_num, table_name, batch, batch_num, host, port, db)
        print(f"All batches submitted for insertion.")
    else:
        print(f"Database file '{file_name}' not found.")

# Function to read from RethinkDB based on the keys in the file
def read_from_db_file(host='localhost', port=28015, db=db_name, table_name='my_table', file_name='db_data.json', batch_size=1000):
    # Connect to the database
    connection, rdb = connect_to_rethinkdb(host, port, db)
    if connection is None:
        print("Failed to connect to RethinkDB for reading.")
        return

    if os.path.exists(file_name):
        print(f"Reading keys from '{file_name}' and fetching data from table '{table_name}' in batches of {batch_size}...")

        with open(file_name, 'r') as f:
            data = json.load(f)
            ids = [entry['id'] for entry in data]

            # Fetch records in batches
            total_records = 0
            for i in range(0, len(ids), batch_size):
                batch_ids = ids[i:i + batch_size]
                results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')

                # Count and print the number of records in the current batch
                batch_records = len(list(results))
                total_records += batch_records
                print(f"Batch {i // batch_size + 1}: {batch_records} records fetched.")

        print(f"{total_records} total records fetched from the table.")
        connection.close()
        print("Reading completed and connection closed.")
    else:
        print(f"Database file '{file_name}' not found.")

def connect_to_multiple_hosts_and_read_round_robin(hosts, port, db, table_name, file_name='db_data.json', batch_size=1000, num_threads=3):
    if not os.path.exists(file_name):
        print(f"Database file '{file_name}' not found.")
        return

    # Define the list of hosts (IP addresses of the RethinkDB instances)
    host_cycle = cycle(hosts)  # Cycle through the hosts for round-robin scheduling

    # Read data from the file
    print(f"Reading data from '{file_name}' and fetching from the table '{table_name}'...")
    with open(file_name, 'r') as f:
        data = json.load(f)
        ids = [entry['id'] for entry in data]

    # Use ThreadPoolExecutor for parallel reads
    total_records = 0
    start_time = datetime.now()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            
            # Get the next host from the cycle (round-robin)
            host = next(host_cycle)
            
            # Submit the batch for reading on the current host
            futures.append(executor.submit(read_batch, host, port, db, table_name, batch_ids, i // batch_size + 1))

        # Process results as they complete
        for future in futures:
            batch_records = future.result()
            total_records += batch_records
            print(f"Batch completed with {batch_records} records fetched.")
    end_time = datetime.now()
    
    total_time = (end_time - start_time).total_seconds()
    print(f"{total_records} total records fetched from the table across hosts: {', '.join(hosts)}.")
    print(f"Time taken: {total_time} seconds.")
    print(f"Throughput: {total_records / total_time} records per second.")

# Helper function for reading a batch from a specific host
def read_batch(host, port, db, table_name, batch_ids, batch_num):
    connection, rdb = connect_to_rethinkdb_per_thread(host=host, port=port, db=db)
    if connection is None:
        print(f"Failed to connect to RethinkDB on {host} for batch {batch_num}.")
        return 0

    try:
        print(f"Fetching batch {batch_num} from host {host}...")
        results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')
        records_count = len(list(results))
        print(f"Batch {batch_num} fetched {records_count} records from host {host}.")
        return records_count
    except Exception as e:
        print(f"Error reading batch {batch_num} from host {host}: {e}")
        return 0
    finally:
        connection.close()

def connect_to_multiple_hosts_and_update_round_robin(hosts, port, db, table_name, file_name='db_data.json', batch_size=1000, num_threads=3, update_percentage=0.1):
    if not os.path.exists(file_name):
        print(f"Database file '{file_name}' not found.")
        return

    # Define the list of hosts (IP addresses of the RethinkDB instances)
    host_cycle = cycle(hosts)  # Cycle through the hosts for round-robin scheduling

    # Read data from the file
    print(f"Reading data from '{file_name}' to update keys in the table '{table_name}'...")
    with open(file_name, 'r') as f:
        data = json.load(f)
        ids = [entry['id'] for entry in data]

    # Calculate the number of records to update (10% of the dataset)
    num_updates = int(len(ids) * update_percentage)
    ids_to_update = random.sample(ids, num_updates)

    # Use ThreadPoolExecutor for parallel updates
    total_updated = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(0, len(ids_to_update), batch_size):
            batch_ids = ids_to_update[i:i + batch_size]
            
            # Get the next host from the cycle (round-robin)
            host = next(host_cycle)
            
            # Submit the batch for updating on the current host
            futures.append(executor.submit(update_batch, host, port, db, table_name, batch_ids, i // batch_size + 1))

        # Process results as they complete
        for future in futures:
            batch_updated = future.result()
            total_updated += batch_updated
            print(f"Batch completed with {batch_updated} records updated.")

    print(f"{total_updated} total records updated in the table across hosts: {', '.join(hosts)}.")

# Helper function for updating a batch from a specific host
def update_batch(host, port, db, table_name, batch_ids, batch_num):
    connection, rdb = connect_to_rethinkdb_per_thread(host=host, port=port, db=db)
    if connection is None:
        print(f"Failed to connect to RethinkDB on {host} for batch {batch_num}.")
        return 0

    try:
        print(f"Updating batch {batch_num} on host {host}...")
        records_updated = 0

        # Fetch the original records
        results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')

        for record in results:
            new_value = generate_new_value_same_size(record)  # Generate new value with same size as old value
            rdb.table(table_name).get(record['id']).update({'value': new_value}).run(connection)
            records_updated += 1

        print(f"Batch {batch_num} updated {records_updated} records on host {host}.")
        return records_updated
    except Exception as e:
        print(f"Error updating batch {batch_num} on host {host}: {e}")
        return 0
    finally:
        connection.close()

# Helper function to generate a new value with the same size as the old value
def generate_new_value_same_size(record):
    old_value = record['value']
    new_value = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(len(old_value)))
    return new_value

def load_from_files(directory, host='localhost', port=28015, db=db_name, table_name='my_table', batch_size=1000):
    # Get list of files matching pattern 'run.load.*'
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.startswith('run.load.')]
    num_threads = len(files)
    print(f"Found {num_threads} files to process.")

    # Use ThreadPoolExecutor to process files in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for thread_id, file_path in enumerate(files):
            executor.submit(insert_keys_from_file, thread_id + 1, file_path, host, port, db, table_name, batch_size)

def insert_keys_from_file(thread_id, file_path, host, port, db, table_name, batch_size=1000):
    # Connect to RethinkDB per thread
    connection, rdb = connect_to_rethinkdb_per_thread(host=host, port=port, db=db)
    if connection is None:
        print(f"Thread {thread_id}: Failed to connect to RethinkDB")
        return

    try:
        print(f"Thread {thread_id}: Processing file {file_path}")
        # Read the file and extract keys
        with open(file_path, 'r') as f:
            keys = []
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 1:
                    try:
                        key = int(parts[0])
                        keys.append(key)
                    except ValueError:
                        print(f"Thread {thread_id}: Invalid key '{parts[0]}' in file {file_path}")
            print(f"Thread {thread_id}: Extracted {len(keys)} keys from {file_path}")

        # Batch the keys and insert into the database
        for i in range(0, len(keys), batch_size):
            batch_keys = keys[i:i + batch_size]
            # Construct the records to insert
            batch = [{'id': key, 'value': f"value_{key}".ljust(VALUE_SIZE, '0')} for key in batch_keys]
            rdb.table(table_name).insert(batch).run(connection)
            print(f"Thread {thread_id}: Inserted batch {i // batch_size + 1} with {len(batch)} records into '{table_name}'.")

    except Exception as e:
        print(f"Thread {thread_id}: Error processing file {file_path}: {e}")
    finally:
        connection.close()

def read_from_files(directory, node_to_host, host='localhost', port=28015, db=db_name, table_name='my_table', batch_size=1000, total_records=5000000):
    # Get list of files matching pattern 'client_0_thread_*_clientPerThread_0.txt'
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.startswith('client_0_thread_') and f.endswith('_clientPerThread_0.txt')]
    num_threads = len(files)
    print(f"Found {num_threads} files to process for reads.")

    start_time = datetime.now()
    # Use ThreadPoolExecutor to process files in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for thread_id, file_path in enumerate(files):
            future = executor.submit(read_keys_from_file, thread_id + 1, file_path, node_to_host, host, port, db, table_name, batch_size)
            futures.append(future)

        # Collect results
        for future in futures:
            thread_records = future.result()
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()
    print(f"Total records fetched: {total_records}")
    print(f"Total time taken: {total_time} seconds")
    if total_time > 0:
        throughput = total_records / total_time
        print(f"Throughput: {throughput} records per second")
    else:
        print("Total time is zero, cannot compute throughput")

def read_keys_from_file(thread_id, file_path, node_to_host, host, port, db, table_name, batch_size=100000):
    print(f"Thread {thread_id}: Processing file {file_path}")

    # Read the file and extract keys and node_numbers
    node_keys = {}  # node_number -> list of keys
    try:
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        key = int(parts[0])
                        node_number = int(parts[1])
                        if node_number not in node_keys:
                            node_keys[node_number] = []
                        node_keys[node_number].append(key)
                    except ValueError:
                        print(f"Thread {thread_id}: Invalid line '{line.strip()}' in file {file_path}")
                else:
                    print(f"Thread {thread_id}: Skipping invalid line '{line.strip()}' in file {file_path}")
        # print(f"Thread {thread_id}: Extracted keys for nodes {list(node_keys.keys())}")
    except Exception as e:
        print(f"Thread {thread_id}: Error reading file {file_path}: {e}")
        return

    # Prepare connections per node_number
    connections = {}
    for node_number in node_keys.keys():
        if node_number in node_to_host:
            node_host = node_to_host[node_number]
            connection, rdb = connect_to_rethinkdb_per_thread(host=node_host, port=port, db=db)
            if connection is None:
                print(f"Thread {thread_id}: Failed to connect to RethinkDB on {node_host}")
                connections[node_number] = None
            else:
                connections[node_number] = (connection, rdb)
        else:
            print(f"Thread {thread_id}: Unknown node_number {node_number}")
            connections[node_number] = None

    total_records = 0
    # For each node_number, process the keys
    for node_number, keys in node_keys.items():
        conn_tuple = connections.get(node_number)
        if conn_tuple is None:
            print(f"Thread {thread_id}: No connection available for node {node_number}")
            continue
        connection, rdb = conn_tuple
        node_host = node_to_host[node_number]

        # Process keys in batches
        for i in range(0, len(keys), batch_size):
            batch_keys = keys[i:i + batch_size]
            try:
                # Issue read request for batch_keys
                # print(f"Thread {thread_id}: Fetching batch {i // batch_size + 1} from node {node_number} (host {node_host}) with {len(batch_keys)} keys.")
                results = rdb.table(table_name).get_all(*batch_keys).run(connection, read_mode='outdated')
                records_count = len(list(results))
                total_records += records_count
                # print(f"Thread {thread_id}: Fetched {records_count} records from node {node_number} (host {node_host}).")
            except Exception as e:
                print(f"Thread {thread_id}: Error fetching from node {node_number} (host {node_host}): {e}")
                break  # Stop processing this node if error occurs

    # Close all connections
    for node_number, conn_tuple in connections.items():
        if conn_tuple is not None:
            conn_tuple[0].close()
    print(f"Thread {thread_id}: Completed processing file {file_path}. Total records fetched: {total_records}")

def create_trigger_file():
    with open(trigger_file, 'w') as f:
        f.write("Trigger file created.")

def wait_for_trigger_file():
    while not os.path.exists(trigger_file):
        time.sleep(1)
        pass
    print("Trigger file found.")

def delete_trigger_file():
    if os.path.exists(trigger_file):
        os.remove(trigger_file)
        print("Trigger file deleted.")
    else:
        print("Trigger file not found.")