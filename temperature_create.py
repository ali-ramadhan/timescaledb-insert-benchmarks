import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")

with engine.connect() as conn:
    conn.execute(text("""--sql
        create table if not exists weather (
            id serial primary key,
            time timestamp,
            latitude float,
            longitude float,
            temperature float
        );
    """))
    conn.commit()
