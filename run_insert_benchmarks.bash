#!/bin/bash

set -e

source .env

methods=("pandas" "psycopg3" "sqlalchemy")
table_types=("regular" "hyper")
runs=10

wait_for_db_to_be_ready() {
    while ! docker exec -it -u postgres "$CONTAINER_NAME" pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
        sleep 2
    done
}

for (( i=1; i<=$runs; i++ )); do
    for method in "${methods[@]}"; do
        for table_type in "${table_types[@]}"; do
            set -x

            docker-compose up --detach

            wait_for_db_to_be_ready

            poetry run python create_table.py \
                --drop-table \
                --table-type $table_type

            poetry run python insert_data.py \
                --method $method \
                --table-type $table_type \
                --num-rows 20000 \
                --benchmarks-file "benchmarks_insert.csv"

            docker-compose down

            { set +x; } 2>/dev/null
        done
    done
done

poetry run python plot_insert_benchmarks.py --benchmarks-file "benchmarks_insert.csv"
