from database import common as db
from data import generator
import json
import sys

def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

# TODO - Load from args

table_models = []

dbg = None

def run_commands(list_of_commands):
    # TODO - Change to actual implementation
    print('\n'.join(list_of_commands))

def main(spec):
    global dbg
    table_data_generators = {}
    tables_spec_by_name = {}
    for index, table in enumerate(spec['tables']):
        schema_name, table_name, columns = db.get_table_data(table)
        tables_spec_by_name[table_name] = table
        table_model = db.create_new_table(schema_name, table_name, columns)
        table_models.append(table_model)
        print(f"{index} {table_model}")
        table_data_generators[table_name] = generator.get_row_generators(table)
    db.save_schema()
    print("Database structure created")

    # Build dataset
    dataset = []
    for table_name, generators in table_data_generators.items():
        print(f"Building row generator for table {table_name}")
        # Compose an INSERT generator using existing generators
        columns = []

        for column in generators.keys():
            columns.append(column)

        if not columns:
            print(f"No columns for table {table_name}. Skipping...")
            continue

        # Generate insert commands
        number_of_rows_to_create = 50

        dbg = generators

        def get_new_data_row():
            return tuple([ generators[column]() for column in generators.keys() ])

        generated_data = [ get_new_data_row() for _ in range(number_of_rows_to_create) ]
        dataset.append((table_name, columns, generated_data))

    print("Dataset built")

    # Consume dataset
    # for table_name, columns, rows in dataset:
    #     db.insert_rows(table_name, columns, rows)
    db.insert_tables(dataset)
    print("Database populated")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec)



