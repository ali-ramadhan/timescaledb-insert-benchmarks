#!/bin/bash

set -e

source .env

methods=("pg_bulkload" "timescaledb_parallel_copy")
hypertable_options=("" "--create-hypertable")
runs=10

wait_for_db_to_be_ready() {
    while ! docker exec -it -u postgres "$CONTAINER_NAME" pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
        sleep 2
    done
}

for method in "${methods[@]}"; do
    for (( i=1; i<=$runs; i++ )); do
        for hypertable_option in "${hypertable_options[@]}"; do
            set -x

            docker-compose up --detach

            wait_for_db_to_be_ready

            poetry run python create_table.py \
                --drop-table \
                $hypertable_option

            if [ "$hypertable_option" ]; then
                benchmarks_file="benchmarks_tools_hypertable.csv"
            else
                benchmarks_file="benchmarks_tools_nohypertable.csv"
            fi

            poetry run python load_using_tools.py \
                --hours 1 \
                --benchmarks-file $benchmarks_file \
                --method $method \
                --workers 1

            docker-compose down

            { set +x; } 2>/dev/null
        done
    done
done

merged_csv="benchmarks_tools.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,workers,hour,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_tools_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_tools_hypertable.csv >> "$merged_csv"

poetry run python plot_insert_benchmarks.py --benchmarks-file "$merged_csv"