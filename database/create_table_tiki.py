import psycopg2
from config import load_config
import json

def create_tables():
    """ Create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE tiki (
            id BIGINT PRIMARY KEY,
            name TEXT,
            url_key TEXT,
            price INTEGER,
            description TEXT,
            images JSONB
            );
        """
        )
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                cur.execute(commands)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_tables()
