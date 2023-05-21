from typing import List, Tuple
from sqlalchemy import create_engine
from rich import print

def get_engine(connection_url):
    return create_engine(connection_url)

def get(engine, query):
    """Returns an array of tuples with query results"""
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()