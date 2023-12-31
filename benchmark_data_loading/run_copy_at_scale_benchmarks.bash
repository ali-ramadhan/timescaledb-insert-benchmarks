#!/bin/bash

set -e

source .env

methods=("psycopg3" "copy_csv")
hypertable_options=("" "--create-hypertable")

wait_for_db_to_be_ready() {
    while ! docker exec -it -u postgres "$CONTAINER_NAME" pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
        sleep 2
    done
}

for method in "${methods[@]}"; do
    for hypertable_option in "${hypertable_options[@]}"; do
        set -x

        docker-compose up --detach

        wait_for_db_to_be_ready

        poetry run python create_table.py \
            --drop-table \
            $hypertable_option

        if [ "$hypertable_option" ]; then
            benchmarks_file="benchmarks_copy_at_scale_hypertable.csv"
        else
            benchmarks_file="benchmarks_copy_at_scale_nohypertable.csv"
        fi

        poetry run python copy_data.py \
            --hours 744 \
            --benchmarks-file $benchmarks_file \
            --method $method \
            --workers 1

        docker-compose down

        { set +x; } 2>/dev/null
    done
done

merged_csv="benchmarks_copy_at_scale.csv"

if [ -f "$merged_csv" ]; then
    rm "$merged_csv"
fi

echo "method,workers,hour,num_rows,seconds_full,rate_full,units_full,seconds_copy,rate_copy,units_copy,hypertable" > "$merged_csv"
awk 'NR > 1 {print $0",false"}' benchmarks_copy_at_scale_nohypertable.csv >> "$merged_csv"
awk 'NR > 1 {print $0",true"}' benchmarks_copy_at_scale_hypertable.csv >> "$merged_csv"
