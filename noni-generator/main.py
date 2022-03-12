from database import common as db
from data import generator
import json
import sys

def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

# TODO - Load from args

table_models = []

def main(spec):
    """
    # Test connection
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM products")
            for result in cur:
                print(result)
    """
    for index, table in enumerate(spec['tables']):
        schema_name, table_name, columns = db.get_table_data(table)
        table_model = db.create_new_table(schema_name, table_name, columns)
        table_models.append(table_model)
        print(f"{index} {table_model}")
        """
        for column in table['columns']:
            sato_category = '' if not column['metadata'] else column['metadata']['columnData']['satoCategory'] if 'satoCategory' in column['metadata']['columnData'] else ''
            print(f"    {column['name']} {column['type']}({column['nativeType']}) {'[M] ?> ' + str(sato_category) if column['metadata'] != None else ''}")
        print()
        """
    db.save_schema()

    created_data = generator.create_data_for_table(table_models[0])
    print(created_data)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec)



