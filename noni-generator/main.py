import json
import sys

from rich import print

from database import common as db
from data import generator


def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

# TODO - Load from args

table_models = []

dbg = None

def run_commands(list_of_commands):
    # TODO - Change to actual implementation
    print('\n'.join(list_of_commands))

def get_ordered_tables(tables_list):
    # Rebuilding table filling algorithm


    # First, topologically sort tables to consider their FK -> PK relations
    working_stack = [] # push with append, top is [-1]
    output_stack = []
    visited = set()
    tables = { (t['schema'],t['name']) : t for t in tables_list }
    table_index = 0
    number_of_tables = len(tables_list)

    while True:
        # print(f"w {working_stack} o {output_stack} v {visited} i {table_index} n {number_of_tables}")
        if not working_stack:
            if len(visited) == number_of_tables:
                break # done
            working_stack.append(tables_list[table_index])
            table_index += 1
        top_of_stack = (working_stack[-1]['schema'],working_stack[-1]['name'])
        if not top_of_stack in visited:
            visited.add(top_of_stack)
            output_stack.append(top_of_stack)
            for fk in tables[top_of_stack]['constraints']['foreign_key']:
                dependency = (fk['refer_schema'], fk['refer_table'])
                if not dependency in visited:
                    working_stack.append(tables[dependency])
        else:
            working_stack.pop()

    print(output_stack)
    exit()



def main(spec):
    global dbg
    table_data_generators = {}
    tables_spec_by_name = {}

    
    # Topologically order tables to allow creating in the right dependency order
    ordered_tables = get_ordered_tables(spec['tables'])

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



