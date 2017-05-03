#!/bin/bash -e

levels=( MEMORY_ONLY MEMORY_ONLY_SER DISK_ONLY MEMORY_AND_DISK )

mkdir out

# for points in $(seq 1000000 1000000 10000000); do
for points in $(seq 10000000 5000000 30000000); do
    # Change the configuration and generate data
    sed -i "s/\(NUM_OF_POINTS=\).*/\1$points/" conf/env.sh
    ./bin/gen_data.sh

    for level in "${levels[@]}"; do
        sed -i "s/\(STORAGE_LEVEL=\).*/\1$level/" ../conf/env.sh
        ./bin/run.sh
    done
done
