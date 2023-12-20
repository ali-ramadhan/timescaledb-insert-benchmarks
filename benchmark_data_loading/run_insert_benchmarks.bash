#!/bin/bash

methods=("pandas" "psycopg3" "sqlalchemy")

for method in "${methods[@]}"; do
    for i in {1..10}; do
        set -x

        poetry run python create_table.py \
            --drop-table &&
        poetry run python insert_data.py \
            --benchmark-file benchmark_insert_nohypertable.csv \
            --num-rows 10000 \
            --method $method

        poetry run python create_table.py \
            --drop-table \
            --create-hypertable &&
        poetry run python insert_data.py \
            --benchmark-file benchmark_insert_hypertable.csv \
            --num-rows 10000 \
            --method $method

        set +x
    done
done

merged_csv="benchmark_insert.csv"
echo "method,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmark_insert_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmark_insert_hypertable.csv >> "$merged_csv"
rm benchmark_insert_nohypertable.csv
rm benchmark_insert_hypertable.csv
