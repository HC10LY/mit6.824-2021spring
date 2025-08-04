#!/bin/bash

repeat=30

mkdir -p logs

for ((i=1; i<=repeat; i++)); do
  log_file="logs/test_run${i}.log"
  echo "Running full go test without race, iteration #$i"
  go test > "$log_file" 2>&1
  if [ $? -eq 0 ]; then
    echo "  Run #$i SUCCESS"
  else
    echo "  Run #$i FAILED, see $log_file"
  fi
done

echo "All runs finished."