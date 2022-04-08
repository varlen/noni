from database import common as db
from data import generator
import json
import sys

def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

# TODO - Load from args

table_models = []

def run_commands(list_of_commands):
    # TODO - Change to actual implementation
    print('\n'.join(list_of_commands))

def main(spec):
    """
    # Test connection
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM products")
            for result in cur:
                print(result)
    """
    table_data_generators = {}
    tables_spec_by_name = {}
    for index, table in enumerate(spec['tables']):
        schema_name, table_name, columns = db.get_table_data(table)
        tables_spec_by_name[table_name] = table
        table_model = db.create_new_table(schema_name, table_name, columns)
        table_models.append(table_model)
        print(f"{index} {table_model}")
        table_data_generators[table_name] = generator.get_row_generators(table)
        """
        for column in table['columns']:
            sato_category = '' if not column['metadata'] else column['metadata']['columnData']['satoCategory'] if 'satoCategory' in column['metadata']['columnData'] else ''
            print(f"    {column['name']} {column['type']}({column['nativeType']}) {'[M] ?> ' + str(sato_category) if column['metadata'] != None else ''}")
        print()
        """
    db.save_schema()

    db_commands = []

    for table_name, generators in table_data_generators.items():
        # Compose an INSERT generator using existing generators
        columns = []

        for column in generators.keys():
            columns.append(column)

        if not columns:
            print(f"No columns for table {table_name}. Skipping...")
            continue

        # Generate insert commands
        rows_to_create = 50

        def get_new_data_row():
            return [ generators[column]() for column in generators.keys() ]

        generated_data = [ get_new_data_row() for _ in range(rows_to_create) ]

        db_commands.append(db.create_insert_command(table_name, columns, generated_data))

    db.execute_commands(db_commands)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec)



