from database.common import create_new_table, save_schema, connection
import json
import sys

def load_spec_file(path):
    with open(path) as spec_file:
        return json.load(spec_file)

# TODO - Load from args

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
        print(f"{index}){table['schema']}.{table['name']}")
        for column in table['columns']:
            sato_category = '' if not column['metadata'] else column['metadata']['columnData']['satoCategory'] if 'satoCategory' in column['metadata']['columnData'] else ''
            print(f"    {column['name']} {column['type']}({column['nativeType']}) {'[M] ?> ' + str(sato_category) if column['metadata'] != None else ''}")
        print()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec)



