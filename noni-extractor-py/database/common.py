from typing import List, Tuple
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects import postgresql
from rich import print

DEFAULT_CONNECTION_URL = "postgresql://pguser:password@localhost:5432/mydb"

def get_engine(connection_url = DEFAULT_CONNECTION_URL):
    return create_engine(connection_url)

metadata_context = MetaData()

string_to_type = {
    "smallint":postgresql.SMALLINT,
    "character varying":postgresql.VARCHAR,
    "character":postgresql.VARCHAR,
    "text":postgresql.TEXT,
    "integer":postgresql.INTEGER,
    "real":postgresql.REAL,
    "date":postgresql.DATE,
    "bytea":postgresql.BYTEA,
    "boolean":postgresql.BOOLEAN,
    "double":postgresql.DOUBLE_PRECISION,
    "uuid":postgresql.UUID,
    "bigint":postgresql.BIGINT,
    "timestamp":postgresql.TIMESTAMP
}

def get_type(type_string):
    if type_string in string_to_type:
        return string_to_type[type_string]
    raise Exception(f"Type {type_string} has no SQLAlchemy type mapping")

def create_insert_command(
    table_name : str,
    columns : List[str]
    ) -> str:
    value_placeholder =  f"({', '.join([ '%s' for _ in columns ])})"
    return (f"INSERT INTO {table_name} ({ ', '.join(columns) }) VALUES {value_placeholder}")

def get_table_data(spec_table):
    """
    Returns a tuple with the schema name, table name and column data
    """
    column_data = [
        (column['name'], get_type(column['nativeType']), column['type'] == 'key')
        for column in spec_table['columns']
    ]
    return (spec_table['schema'], spec_table['name'], column_data)

def create_new_table(schema_name, table_name, column_data):
    return Table(
        table_name,
        metadata_context,
        schema=schema_name,
        *[Column(column_name, column_type, primary_key=is_pk) for column_name, column_type, is_pk in column_data],
    )

def save_schema(engine):
    metadata_context.create_all(engine)

def insert_tables(dataset):
    own_engine = get_engine()
    conn = own_engine.raw_connection()
    for table_name, columns, rows in dataset:
        insert_command = create_insert_command(table_name, columns)
        with conn.cursor() as cur:
            print(f'[cyan]{insert_command}[/cyan]')
            for row in rows:
                print(f'  {row}')
                try:
                    cur.execute(insert_command, row)
                except:
                    print(f"[red][ERROR] {insert_command} {row}[/red]")
                    raise
    conn.commit()
    conn.close()

def insert_rows(engine, table_name : str, columns : str, data : List[Tuple]):
    insert_command = create_insert_command(table_name, columns)
    with engine.raw_connection() as conn:
        with conn.cursor() as cur:
            for row in data:
                cur.execute(insert_command, row)

def get(engine, query):
    """Returns an array of tuples with query results"""
    #print(engine)
    #print(engine.name)
    #print(dir(engine))
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def example(engine):
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