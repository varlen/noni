import json
import sys

from rich import print

from database import common as db
from data import generator


def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

table_models = []

dbg = None

def get_ordered_tables(tables_list):
    # First, topologically sort tables to consider their FK -> PK relations
    working_stack = [] # push with append, top is [-1]
    output_stack = []
    visited = set()
    tables = { (t['schema'],t['name']) : t for t in tables_list }
    table_index = 0
    number_of_tables = len(tables_list)

    while True:
        if not working_stack:
            if len(visited) == number_of_tables:
                break # ordering finished
            # in the list of tables to order, check if the next was already visited
            next_candidate = tables_list[table_index]
            table_index += 1
            if (next_candidate['schema'], next_candidate['name']) in visited:
                continue
            working_stack.append(next_candidate)
        top_of_stack = (working_stack[-1]['schema'],working_stack[-1]['name'])
        # if top of stack not visited yet, visit and stack dependencies
        if not top_of_stack in visited:
            visited.add(top_of_stack)
            for fk in tables[top_of_stack]['constraints']['foreign_key']:
                dependency = (fk['refer_schema'], fk['refer_table'])
                if not dependency in visited:
                    working_stack.append(tables[dependency])
        # if visited, then move to output stack
        else:
            ordered_table = working_stack.pop()
            output_stack.append((ordered_table['schema'], ordered_table['name']))
    return output_stack, tables

def main(spec):
    global dbg
    table_data_generators = {}

    # Topologically order tables to allow creating in the right dependency order
    ordered_tables, tables_dict = get_ordered_tables(spec['tables'])

    foreign_key_data_sources = {}

    for schema, table in ordered_tables:
        table_metadata = tables_dict[(schema, table)]
        columns = db.get_column_data(table_metadata)
        table_model = db.create_new_table(schema, table, columns)
        table_models.append(table_model)
        table_data_generators[(schema, table)] = generator.get_row_generators(table_metadata, foreign_key_data_sources)

    db.save_schema()
    print("Database structure created")
    print(ordered_tables)

    # Build dataset
    dataset = []
    for schema, table in ordered_tables:
        generators = table_data_generators[(schema, table)]
        print(f"Building row generator for table {schema}.{table}")
        # Compose an INSERT generator using existing generators
        columns = []

        for column in generators.keys():
            columns.append(column)

        if not columns:
            print(f"No columns for table {schema}.{table}. Skipping...")
            continue

        # Generate insert commands
        number_of_rows_to_create = 50

        dbg = generators

        print('Generators', generators)
        def generate_data(column):
            data = generators[column]()
            if (schema, table, column) in foreign_key_data_sources:
                foreign_key_data_sources[(schema, table, column)].append(data)
            return data

        def get_new_data_row():
            return tuple([ generate_data(column) for column in generators.keys() ])

        generated_data = [ get_new_data_row() for _ in range(number_of_rows_to_create) ]
        dataset.append((f'{schema}.{table}', columns, generated_data))

    print(dataset)
    exit()

    print("Dataset built")

    # Consume dataset
    # for table_name, columns, rows in dataset:
    #     db.insert_rows(table_name, columns, rows)
    #db.insert_tables(dataset)
    print("Database populated")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec)



