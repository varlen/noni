import time
import sys
import os
import database.common as db
import database.plugins as db_plugins
import json
import core
from rich import print

"""
Currently using the same venv as generator when testing
"""

def validate_database_type(database_type):
    valid_database_types = db_plugins.get_dialects()
    return database_type in valid_database_types

def validate_input_parameters(database_type):
    if not database_type:
        raise Exception("Please provide the database type")
    if not validate_database_type(database_type):
        raise Exception(f"No implementation available for '{database_type}' database.")

def main(source_database_url = None, database_type = None, output_file_name = "output2.json"):
    validate_input_parameters(database_type)
    # Start timer
    start_time_seconds = time.perf_counter()
    # Connect to DB
    db_engine = None
    if not source_database_url:
        print(f"[red]Please provide a source database url in a format compatible with SQLAlchemy.[/red]")
        exit()
    else:
        db_engine = db.get_engine(source_database_url)

    # Load proper implementation to talk with db
    db_plugin = db_plugins.load_dialect(database_type)

    # Extract database structure
    database_structure = core.get_database_structure(db_engine, db, db_plugin)

    with open('checkpoint.json', 'w') as f:
        json.dump(database_structure, f, indent=2)

    core.add_metadata(db_engine, db, database_structure, db_plugin)

    print(database_structure)

    with open(output_file_name, 'w') as f:
        json.dump(database_structure, f, indent=2, default=str)
        print(f"[cyan]Database structure exported to {output_file_name}[/cyan]")

    elapsed_time_seconds = time.perf_counter() - start_time_seconds
    print(f"Done. Elapsed time: {elapsed_time_seconds} s")

if __name__ == "__main__":
    if '--help' in sys.argv:
        print("""Noni Extractor
        --help              display this text
        --list-dialects     list available dialects for extractor""")
    elif '--list-dialects' in sys.argv:
        print('\n'.join(db_plugins.get_dialects()))
    else:
        CONNECTION_URL = os.environ['INPUT_DATABASE_URL'] \
            if 'INPUT_DATABASE_URL' in os.environ\
            else None
        DIALECT = os.environ['DB_DIALECT'] \
            if 'DB_DIALECT' in os.environ\
            else "postgres"
        OUTPUT_FILE_NAME = os.environ['OUTPUT_FILE'] \
            if 'OUTPUT_FILE' in os.environ \
            else 'output.json'
        main(source_database_url=CONNECTION_URL, database_type=DIALECT, output_file_name=OUTPUT_FILE_NAME)