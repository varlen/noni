from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

# TODO - Load from environment
CONNECTION_URL = "postgresql://pguser:password@localhost:5432/mydb"

engine = create_engine(CONNECTION_URL)
metadata_context = MetaData()

def create_new_table(table_name, column_data):
    return Table(
        table_name,
        metadata_context,
        **[Column(column_name, column_type, primary_key=is_pk) for column_name, column_type, is_pk in column_data]
    )

def save_schema():
    metadata_context.create_all(engine)

def connection():
    return engine.raw_connection()

def example():
    with engine.raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM products')
    # Connect to an existing database
    with engine.raw_connection() as conn:

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