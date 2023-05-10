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

def main(spec, build_structure=True, populate=True, print_dataset=False):
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

    if build_structure:
        db.save_schema()
        # Add foreign keys to database
        for schema, table in ordered_tables:
            table_metadata = tables_dict[(schema, table)]
            if 'constraints' in table_metadata and 'foreign_key' in table_metadata['constraints'] \
                and len(table_metadata['constraints']['foreign_key']):
                constraints = table_metadata['constraints']
                fk_dict = {}
                for fk_data in constraints['foreign_key']:
                    if not fk_data['name'] in fk_dict:
                        fk_dict[fk_data['name']] = {
                            'cols' : [fk_data['column']],
                            'ref_schema' : fk_data['refer_schema'],
                            'ref_table' : fk_data['refer_table'],
                            'ref_cols' : [fk_data['refer_column']]
                        }
                    else:
                        fk_dict[fk_data['name']]['cols'].append(fk_data['column'])
                        fk_dict[fk_data['name']]['ref_cols'].append(fk_data['refer_column'])
                for fk_name, fk_details in fk_dict.items():
                    cols = ','.join(fk_details['cols'])
                    ref_cols = ','.join(fk_details['ref_cols'])
                    full_ref_table_name = f'{fk_details["ref_schema"]}.{fk_details["ref_table"]}'
                    db.register_foreign_keys(table_metadata['name'], fk_name, cols,
                                             full_ref_table_name, ref_cols)
                    print(f"  [green]Created foreign key {fk_name}[/green]")
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
        number_of_rows_to_create = 5 # TODO - Change

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

        # Validate composite PKs in the data
        table_metadata = tables_dict[(schema, table)]
        primary_key = table_metadata['constraints']['primary_key']
        if len(primary_key) > 1:
            # load primary key details
            primary_key_detail = sorted(list({
                (key_row['name'], key_row['column'], key_row['order'])
                for key_row in primary_key
            }), key=lambda row : row[2])

            # map pk columns to column indexes
            table_column_names = list(map(lambda t: t['name'], table_metadata['columns']))
            pk_column_indexes = [
                table_column_names.index(pk_column[1])
                for pk_column in primary_key_detail
            ]

            # iterate the generated dataset to check for duplications
            existing_pks = set()
            def deduplicate_pk(row):
                pk_entry_buffer = []
                for i in pk_column_indexes:
                    pk_entry_buffer.append(row[i])
                pk_entry = tuple(pk_entry_buffer)
                if pk_entry in existing_pks:
                    return False
                else:
                    existing_pks.add(pk_entry)
                    return True

            # filter to remove duplicated pks
            filtered_data = list(filter(deduplicate_pk, generated_data))
            if len(filtered_data):
                duplications = len(generated_data) - len(filtered_data)
                print(f"[WARN] {duplications} row(s) filtered out due to primary key duplication")
                generated_data = filtered_data

        dataset.append((f'{schema}.{table}', columns, generated_data))

    if print_dataset:
        print(dataset)

    print("Dataset built")

    if populate:
        # Consume dataset
        try:
            for table_name, columns, rows in dataset:
                print(f"  [purple]Populating table [white]{table_name}[/white][/purple]")
                db.insert_rows(table_name, columns, rows)
            # db.insert_tables(dataset)
            print("Database populated")
        except Exception as e:
            print("[red]ERROR[/red]")
            print(e)
            print(foreign_key_data_sources)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Expected spec file path as argument")
        exit(-1)
    spec = load_spec_file(sys.argv[1])
    main(spec,
         build_structure= '--structure' in sys.argv,
         populate='--data' in sys.argv,
         print_dataset='--print-data' in sys.argv)



