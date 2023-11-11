import os
import psycopg

from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

connection_string = (
    f"host={POSTGRES_HOST} "
    f"dbname={POSTGRES_DB_NAME} "
    f"user={POSTGRES_USER} "
    f"password={POSTGRES_PASSWORD}"
)

with psycopg.connect(connection_string) as conn:
    with conn.cursor() as cur:
        cur.execute(
        """--sql
            create table if not exists test (
                id serial primary key,
                num integer,
                data varchar
            );
        """)

        cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, 'abc'))
        cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (200, 'def'))
        cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (300, 'ghi'))

        conn.commit()
