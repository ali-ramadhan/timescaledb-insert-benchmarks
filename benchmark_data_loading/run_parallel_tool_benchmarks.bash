#!/bin/bash

set -e

source .env

methods=("pg_bulkload" "timescaledb_parallel_copy")
table_types=("regular" "hyper")
num_workers=(1 2 4 8 12 16 24 32 42)

wait_for_db_to_be_ready() {
    while ! docker exec -it -u postgres "$CONTAINER_NAME" pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
        sleep 2
    done
}

for method in "${methods[@]}"; do
    for table_type in "${table_types[@]}"; do
        for workers in "${num_workers[@]}"; do
            set -x

            docker-compose up --detach

            wait_for_db_to_be_ready

            poetry run python create_table.py \
                --drop-table \
                --table-type $table_type

            poetry run python load_using_tools.py \
                --method $method \
                --table-type $table_type \
                --hours 128 \
                --workers $workers \
                --benchmarks-file "benchmarks_parallel_tools_workers.csv" \
                --parallel-benchmarks-file "benchmarks_parallel_tools.csv"

            docker-compose down

            { set +x; } 2>/dev/null
        done
    done
done
