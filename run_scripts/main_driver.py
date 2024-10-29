import argparse

from rethinkdb_helper import *

hosts = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
node_to_host = {
    1: "10.10.1.1",
    2: "10.10.1.2",
    3: "10.10.1.3",
}

load_db = False
# load_db = True

# load_ycsb = True
load_ycsb = False

db_name = 'test-rac'
batch_sizes = 10000
total_records = 5000000

directory_load = '/mydata/workloads/100k_5M_ops/load'

base_directory = '/mydata/workloads/100k_5M_ops/'

def load_values_into_db(directory_run, batch_size, total_records, leader=False):
    # Step 1: Connect to RethinkDB
    connection, rdb = connect_to_rethinkdb()
    table_name = 'my_table'

    if connection and rdb:
        if load_db:
            delete_existing_tables(connection, rdb)
            create_table(connection, rdb, table_name)
            generate_db_file('db_data.json', 1000000)
            insert_from_db_file(host='localhost', port=28015, db=db_name, table_name=table_name, num_threads=10)
        if load_ycsb:
          delete_existing_tables(connection, rdb)
          create_table(connection, rdb, table_name)
          load_from_files(directory_load, host='localhost', port=28015, db=db_name, table_name=table_name)
          create_trigger_file()

          


        # read_from_db_file(host='localhost', port=28015, db=db_name, table_name=table_name)

        # connect_to_multiple_hosts_and_read_round_robin(hosts, port=28015, db=db_name, table_name='my_table')
        
        # connect_to_multiple_hosts_and_update_round_robin(hosts, port=28015, db=db_name, table_name='my_table')

        wait_for_trigger_file()
        read_from_files(directory_run, node_to_host, host='localhost',
                        port=28015, db=db_name, table_name=table_name,
                        batch_size=batch_sizes, total_records=total_records)
        connection.close()
        delete_trigger_file()
        print("Connection closed.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load and run YCSB workloads into RethinkDB.")
    parser.add_argument('--workload', choices=['zipfian', 'uniform', 'hotspot'], required=True,
                        help="Workload type to run: zipfian, uniform, or hotspot.")
    parser.add_argument('--batch_size', type=int, default=10000,
                        help="Batch size for operations. Default is 10000.")
    parser.add_argument('--total_records', type=int, default=5000000,
                        help="Total records to operate on. Default is 5000000.")
    parser.add_argument('--leader', action='store_true', help="Whether the node is the leader.")
    
    args = parser.parse_args()
    
    directory_run = f"{base_directory}{args.workload}"
    if args.leader:
        load_ycsb = True
    
    # Call the function with parsed arguments
    load_values_into_db(directory_run, args.batch_size, args.total_records, args.leader)