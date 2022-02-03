import psycopg
from psycopg_pool import ConnectionPool

# TODO - Load from environment
CONNECTION_URL = "postgres://pguser:password@localhost:5432/mydb"

pool = ConnectionPool(CONNECTION_URL)

def example():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM products')
    # Connect to an existing database
    with pool.connection() as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Pass data to fill a query placeholders and let Psycopg perform
            # the correct conversion (no SQL injections!)
            cur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))

            # Query the database and obtain data as Python objects.
            cur.execute("SELECT * FROM test")
            cur.fetchone()
            # will return (1, 100, "abc'def")

            # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
            # of several records, or even iterate on the cursor
            for record in cur:
                print(record)

            # Make the changes to the database persistent
            conn.commit()