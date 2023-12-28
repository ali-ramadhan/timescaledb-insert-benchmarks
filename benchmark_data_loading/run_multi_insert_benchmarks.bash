#!/bin/bash

source .env

methods=("pandas" "psycopg3" "sqlalchemy")
runs=10

for method in "${methods[@]}"; do
    for (( i=1; i<=$runs; i++ )); do
        set -x

        docker-compose up --detach

        while ! pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
            sleep 2
        done

        poetry run python create_table.py \
            --drop-table &&
        poetry run python batch_insert_data.py \
            --benchmarks-file benchmarks_batch_insert_nohypertable.csv \
            --num-rows 50000 \
            --method $method

        poetry run python create_table.py \
            --drop-table \
            --create-hypertable &&
        poetry run python batch_insert_data.py \
            --benchmarks-file benchmarks_batch_insert_hypertable.csv \
            --num-rows 50000 \
            --method $method

        docker-compose down

        { set +x; } 2>/dev/null
    done
done

merged_csv="benchmarks_multi_insert.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_multi_insert_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_multi_insert_hypertable.csv >> "$merged_csv"
rm benchmarks_multi_insert_nohypertable.csv
rm benchmarks_multi_insert_hypertable.csv

poetry run python plot_insert_benchmarks.py --benchmarks-file "$merged_csv"
