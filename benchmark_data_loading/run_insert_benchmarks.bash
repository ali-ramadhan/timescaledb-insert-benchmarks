#!/bin/bash

set -e

source .env

methods=("pandas" "psycopg3" "sqlalchemy")
hypertable_options=("" "--create-hypertable")
runs=10

for method in "${methods[@]}"; do
    for (( i=1; i<=$runs; i++ )); do
        for hypertable_option in "${hypertable_options[@]}"; do
            set -x

            docker-compose up --detach

            while ! pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
                sleep 2
            done

            poetry run python create_table.py \
                --drop-table \
                $hypertable_option

            if [ -z "$hypertable_option" ]; then
                benchmarks_file="benchmarks_insert_nohypertable.csv"
            else
                benchmarks_file="benchmarks_insert_hypertable.csv"
            fi

            poetry run python insert_data.py \
                --benchmarks-file $benchmarks_file \
                --num-rows 10000 \
                --method $method

            docker-compose down

            { set +x; } 2>/dev/null
        done
    done
done


merged_csv="benchmarks_insert.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,num_rows,seconds,rate,units,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_insert_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_insert_hypertable.csv >> "$merged_csv"
rm benchmarks_insert_nohypertable.csv
rm benchmarks_insert_hypertable.csv

poetry run python plot_insert_benchmarks.py --benchmarks-file "$merged_csv"
