#!/bin/bash

methods=("pandas" "psycopg3" "sqlalchemy")

for method in "${methods[@]}"; do
    for i in {1..10}; do
        set -x

        poetry run python create_table.py \
            --drop-table &&
        poetry run python batch_insert_data.py \
            --benchmark-file benchmarks_batch_insert_nohypertable.csv \
            --num-rows 50000 \
            --method $method

        poetry run python create_table.py \
            --drop-table \
            --create-hypertable &&
        poetry run python batch_insert_data.py \
            --benchmark-file benchmarks_batch_insert_hypertable.csv \
            --num-rows 50000 \
            --method $method

        set +x
    done
done

merged_csv="benchmarks_batch_insert.csv"
echo "method,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_batch_insert_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_batch_insert_hypertable.csv >> "$merged_csv"
rm benchmarks_batch_insert_nohypertable.csv
rm benchmarks_batch_insert_hypertable.csv

poetry run python plot_insert_benchmarks.py --benchmarks-file "$merged_csv"
