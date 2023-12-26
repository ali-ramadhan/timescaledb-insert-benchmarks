#!/bin/bash

methods=("psycopg3" "copy_csv")

for method in "${methods[@]}"; do
    for i in {1..10}; do
        set -x

        poetry run python create_table.py \
            --drop-table &&
        poetry run python copy_data.py \
            --hours 1 \
            --benchmarks-file benchmarks_copy_nohypertable.csv \
            --method $method

        poetry run python create_table.py \
            --drop-table \
            --create-hypertable &&
        poetry run python copy_data.py \
            --hours 1 \
            --benchmarks-file benchmarks_copy_hypertable.csv \
            --method $method

        set +x
    done
done

merged_csv="benchmarks_copy.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,hour,num_rows,seconds_full,rate_full,units_full,seconds_copy,rate_copy,units_copy,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_copy_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_copy_hypertable.csv >> "$merged_csv"
rm benchmarks_copy_nohypertable.csv
rm benchmarks_copy_hypertable.csv
