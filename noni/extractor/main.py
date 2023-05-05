import time
import sys
import database.common as db
import json
from rich import print

from dialects import postgres as db_interface

"""
Currently using the same venv as generator when testing
"""

def validate_database_type(database_type):
    # TODO - Change implementation to consider existing py files instead of a hardcoded list
    valid_database_types = ["postgres"]
    return database_type in valid_database_types

def validate_input_parameters(database_type):
    if not database_type:
        raise Exception("Please provide the database type")
    if not validate_database_type(database_type):
        raise Exception(f"No implementation available for '{database_type}' database.")

def main(source_database_url = None, database_type = "postgres", output_file_name = "output2.json"):
    validate_input_parameters(database_type)
    # Start timer
    start_time_seconds = time.perf_counter()
    # Connect to DB
    db_engine = None
    if not source_database_url:
        print(f"[yellow]Using default database connection url[/yellow]")
        db_engine = db.get_engine(db.DEFAULT_CONNECTION_URL)
    else:
        db_engine = db.get_engine(source_database_url)

    # Extract database structure
    ## TODO - Dinamically load db_interface according to dialect
    database_structure = db_interface.get_database_structure(db_engine, db)

    with open('checkpoint.json', 'w') as f:
        json.dump(database_structure, f, indent=2)

    db_interface.add_metadata(db_engine, db, database_structure)

    print(database_structure)

    with open(output_file_name, 'w') as f:
        json.dump(database_structure, f, indent=2, default=str)
        print(f"[cyan]Database structure exported to {output_file_name}[/cyan]")

    elapsed_time_seconds = time.perf_counter() - start_time_seconds
    print(f"Done. Elapsed time: {elapsed_time_seconds} s")

if __name__ == "__main__":
    # TODO - Refactor
    if len(sys.argv) > 1:
        main(sys.argv[1])
    main(None)