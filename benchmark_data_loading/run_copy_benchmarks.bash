#!/bin/bash

methods=("psycopg3" "sqlalchemy")

for method in "${methods[@]}"; do
    for i in {1..10}; do
        set -x

        poetry run python create_table.py \
            --drop-table &&
        poetry run python copy_data.py \
            --benchmarks-file benchmarks_copy_nohypertable.csv \
            --csv-filepath /mnt/tamriel/climate/csv/weather_hour0.csv \
            --method $method

        poetry run python create_table.py \
            --drop-table \
            --create-hypertable &&
        poetry run python copy_data.py \
            --benchmarks-file benchmarks_copy_hypertable.csv \
            --csv-filepath /mnt/tamriel/climate/csv/weather_hour0.csv \
            --method $method

        set +x
    done
done

merged_csv="benchmarks_copy.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_copy_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_copy_hypertable.csv >> "$merged_csv"
rm benchmarks_copy_nohypertable.csv
rm benchmarks_copy_hypertable.csv

poetry run python plot_insert_benchmarks.py --benchmarks-file "$merged_csv"