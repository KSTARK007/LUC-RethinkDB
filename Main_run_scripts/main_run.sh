#!/bin/bash

# Declare cache percentages and workload types

CACHE_PERCENTAGES=("10" "20" "30" "33.33" "50" "75" "100")
WORKLOADS=("hotspot" "uniform" "zipfian")
IO_THREADS=("unlimited" "1")

# CACHE_PERCENTAGES=("33.33")
# WORKLOADS=("uniform" "zipfian")
# IO_THREADS=("unlimited")

# Main execution loop
for IO_THREAD in "${IO_THREADS[@]}"; do
  echo "Starting iteration for IO Thread: $IO_THREAD"

  for WORKLOAD_TYPE in "${WORKLOADS[@]}"; do
    echo "Starting workload type: $WORKLOAD_TYPE"

    for CACHE_PERCENTAGE in "${CACHE_PERCENTAGES[@]}"; do
      # Run the iteration with a timeout of 1800 seconds (30 minutes)
      if timeout 1800s ./run_iteration.sh "$CACHE_PERCENTAGE" "$IO_THREAD" "$WORKLOAD_TYPE"; then
        echo "Iteration for cache percentage ${CACHE_PERCENTAGE}% with workload $WORKLOAD_TYPE and IO Thread $IO_THREAD completed."
      else
        # Check if the timeout command caused the failure
        exit_status=$?
        if [ $exit_status -eq 124 ]; then
          echo "Iteration for cache percentage ${CACHE_PERCENTAGE}% with workload $WORKLOAD_TYPE and IO Thread $IO_THREAD timed out." >> failed_runs.txt
          echo "Iteration for cache percentage ${CACHE_PERCENTAGE}% with workload $WORKLOAD_TYPE and IO Thread $IO_THREAD failed due to timeout."
        else
          echo "Iteration for cache percentage ${CACHE_PERCENTAGE}% with workload $WORKLOAD_TYPE and IO Thread $IO_THREAD failed with error." >> failed_runs.txt
        fi
        continue  # Move on to the next iteration
      fi
    done
  done
done

echo "All iterations completed successfully!"
