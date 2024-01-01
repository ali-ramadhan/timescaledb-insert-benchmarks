import os
import subprocess

import dotenv
import sqlalchemy
import psycopg

dotenv.load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

def num_rows(hours):
    return hours * 1038240

def sqlalchemy_connection_string():
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}"

def get_sqlalchemy_engine():
    engine = sqlalchemy.create_engine(sqlalchemy_connection_string())
    return engine

def get_psycopg3_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB_NAME} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(connection_string)

def run_in_container(cmd):
    # Use Popen so we can watch output in real-time
    process = subprocess.Popen(
        ["docker", "exec", "-u", POSTGRES_USER, CONTAINER_NAME] + cmd,
        stderr=subprocess.STDOUT
    )
    process.wait()
    return
