import os
from typing import List, Tuple
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects import postgresql
from rich import print

CONNECTION_URL = os.environ['OUTPUT_DATABASE_URL'] if 'OUTPUT_DATABASE_URL' in os.environ\
    else "postgresql://pguser:password@localhost:5432/output"

def get_engine():
    return create_engine(CONNECTION_URL)

engine = get_engine()
metadata_context = MetaData()

string_to_type = {
    "smallint":postgresql.SMALLINT,
    "character varying":postgresql.VARCHAR,
    "character":postgresql.VARCHAR,
    "text":postgresql.TEXT,
    "integer":postgresql.INTEGER,
    "real":postgresql.REAL,
    "numeric": postgresql.NUMERIC,
    "date":postgresql.DATE,
    "bytea":postgresql.BYTEA,
    "boolean":postgresql.BOOLEAN,
    "double":postgresql.DOUBLE_PRECISION,
    "uuid":postgresql.UUID,
    "bigint":postgresql.BIGINT,
    "timestamp":postgresql.TIMESTAMP,
    "timestamp without time zone": postgresql.TIMESTAMP,
    "tsvector": postgresql.TSVECTOR,
    "ARRAY": postgresql.ARRAY(item_type=postgresql.TEXT),
    "USER-DEFINED": postgresql.TEXT # partial support only
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

def get_column_data(spec_table):
    """
    Returns the column data given a table specification
    """
    primary_key = { col['column'] for col in spec_table['constraints']['primary_key']}
    return [
        (column['name'], get_type(column['nativeType']), column['name'] in primary_key)
        for column in spec_table['columns']
    ]

def get_table_data(spec_table):
    """
    Returns a tuple with the schema name, table name and column data
    """
    return (spec_table['schema'], spec_table['name'], get_column_data(spec_table))

def create_new_table(schema_name, table_name, column_data):
    print(f"[purple] Creating table {table_name}[/purple]")
    return Table(
        table_name,
        metadata_context,
        schema=schema_name,
        *[Column(column_name, column_type, primary_key=is_pk) for column_name, column_type, is_pk in column_data],
    )

def save_schema():
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

def insert_rows(table_name : str, columns : str, data : List[Tuple]):
    insert_command = create_insert_command(table_name, columns)
    own_engine = get_engine()
    conn = own_engine.raw_connection()
    with conn.cursor() as cur:
        for row in data:
            print(f'[cyan]{insert_command} <- {row}[/cyan]')
            cur.execute(insert_command, row)
    conn.commit()
    conn.close()

# IMPROVE - Allow receiving List[str] for creating composite foreign keys
def register_foreign_keys(table_name : str, constraint_name : str, column_name : str, ref_table_name : str, ref_column_name : str):
    print(f"[green]  Creating FK {constraint_name}: {table_name}.{column_name} -> {ref_table_name}.{ref_column_name}[/green]")
    alter_command = f"""ALTER TABLE {table_name}
    ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name})
    REFERENCES {ref_table_name} ({ref_column_name})"""
    print(alter_command)
    own_engine = get_engine()
    conn = own_engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute(alter_command)
    conn.commit()
    conn.close()
