# LUC-RethinkDB

## Overview

This is RethinkDB integrated with the Logically Unified Cache (LUC) architecture. RethinkDB is a production NoSQL database that stores schemaless JSON documents and is designed for real-time applications. 

This LUC integration demonstrates that the unified cache approach can be applied to real-world database systems with minimal changes to the existing codebase. The implementation focuses on **read-path optimizations** using LUC's remote cache access capabilities.

## LUC Integration Details

- **Scope**: Read-path optimizations only (write-path not implemented due to time constraints)
- **Changes**: Modifications contained within the cache layer (~2.4KLOC changes to 282KLOC codebase)
- **Approach**: Cache allocation made contiguous for RDMA access, no changes to distributed protocols or storage engine
- **Performance**: 1.3× to 1.9× improvement in read throughput

## Build Instructions

### Prerequisites

Install dependencies (Ubuntu/Debian):
```bash
sudo apt-get install build-essential protobuf-compiler \
    python3 python-is-python3 \
    libprotobuf-dev libcurl4-openssl-dev \
    libncurses5-dev libjemalloc-dev wget m4 g++ libssl-dev
```

### Build Process

```bash
# Configure build with fetch capability
./configure --allow-fetch

# Build with 4 parallel jobs
make -j4

# Install (optional)
sudo make install
```

### Build Output

After successful build, the RethinkDB binary will be available at:
- Release: `./build/release/rethinkdb`  
- Debug: `./build/debug_clang/rethinkdb`

## CloudLab Setup

**⚠️ This system is optimized for CloudLab deployment with shared NAS storage.**

### Prerequisites
- CloudLab cluster with **minimum 4 nodes** of type **xl170** (3 servers + 1 client)  
- RethinkDB binary deployed to all nodes (see deployment options below)
- IP addresses: 10.10.1.1-3 (servers), 10.10.1.4+ (clients)

### Binary Deployment Options

**Option 1: Use CloudLab Shared Directory**
```bash
# Build on one node and place in shared directory
./configure --allow-fetch && make -j4
cp ./build/release/rethinkdb /proj/YOUR_PROJECT_NAME/rethinkdb

# Update scripts to use shared path
RETHINKDB_PATH="/proj/YOUR_PROJECT_NAME/rethinkdb"
```

**Option 2: Copy Binary to Each Node**
```bash
# After building, copy to all nodes
for node in 10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4; do
  scp ./build/release/rethinkdb $node:/mydata/rethinkdb
done

# Use local path in scripts
RETHINKDB_PATH="/mydata/rethinkdb"
```

### Cluster Configuration

1. **Server Setup** (3 nodes: 10.10.1.1, 10.10.1.2, 10.10.1.3)
   ```bash
   # Start RethinkDB cluster (update RETHINKDB_PATH based on your deployment option)
   # Node 1 (primary):
   $RETHINKDB_PATH --bind all --direct-io --cache-size 115 --io-threads 1
   
   # Nodes 2-3 (join cluster):
   $RETHINKDB_PATH --bind all --direct-io --join 10.10.1.1:29015 \
     --cache-size 115 --io-threads 1
   ```

   **Note**: Update the `RETHINKDB_PATH` variable in `Main_run_scripts/run_iteration.sh` to match your chosen deployment path.

2. **Client Setup** (10.10.1.4+)
   ```bash
   # Install Python RethinkDB driver
   pip install rethinkdb
   
   # Run experiments from Main_run_scripts/
   cd Main_run_scripts/
   ./main_run.sh
   ```

### Experiment Configuration

**Available Parameters:**
- **Cache percentages**: 10%, 20%, 30%, 33.33%, 50%, 75%, 100%
- **Workload types**: hotspot, uniform, zipfian
- **IO threads**: 1 (limited) or unlimited
- **Max cache size**: 345MB (configurable in scripts)

**Scripts:**
- `main_run.sh`: Main experiment runner
- `run_iteration.sh`: Single experiment iteration
- `basic.py`: RethinkDB client workload generator
- `analyze_results.py`: Results analysis

### Usage

```bash
cd Main_run_scripts/

# Run all experiments (takes several hours)
./main_run.sh

# Run single iteration
./run_iteration.sh <cache_percentage> <io_thread> <workload_type>
# Example: ./run_iteration.sh 33.33 1 uniform
```

## Configuration

The LUC-specific configurations are handled internally through the modified cache layer. Cache size and workload parameters are controlled through the experiment scripts.