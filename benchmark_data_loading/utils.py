import os

import dotenv
import sqlalchemy
import psycopg

dotenv.load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def get_sqlalchemy_engine():
    engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}"
    )
    return engine

def get_psycopg3_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"dbname={POSTGRES_DB_NAME} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(connection_string)

