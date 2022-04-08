from typing import List
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects import postgresql

# TODO - Load from environment
CONNECTION_URL = "postgresql://pguser:password@localhost:5432/outputdb"

engine = create_engine(CONNECTION_URL)
metadata_context = MetaData()

string_to_type = {
    "smallint":postgresql.SMALLINT,
    "character varying":postgresql.VARCHAR,
    "character":postgresql.CHAR,
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

def execute_commands(list_of_commands):
    # TODO - Execute actual database commands
    print(list_of_commands)

def get_type(type_string):
    if type_string in string_to_type:
        return string_to_type[type_string]
    raise Exception(f"Type {type_string} has no SQLAlchemy type mapping")

def as_sql_fragment(value):
    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return str(value).upper()
    elif value == None:
        return 'NULL'
    else:
        return str(value)

def create_insert_command(
    table_name : str,
    columns : List[str],
    values_set_list : List[List[object]]
    ) -> str:
    value_set = [ f"({', '.join([ as_sql_fragment(v) for v in values ])})" for values in values_set_list ]
    formatted_values = ',\n'.join(value_set)
    return f"INSERT INTO {table_name} ({ ', '.join(columns) }) VALUES\n{formatted_values}"

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