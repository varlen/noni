import random, uuid
from typing import Callable
from datetime import datetime
from itertools import count
from data.textual import type78_generator
from dateutil.parser import parse as parse_date
from rich import print

def null_generator(column) -> Callable:
    if column['type'] == 'key':
        raise Exception('Cannot use NULL generator for key column')
    return lambda : None

def uuid_generator(column) -> Callable:
    return lambda : uuid.uuid4()

def boolean_generator(column) -> Callable:
    return lambda : random.choice([True, False])

def small_int_generator(column) -> Callable:
    if column['type'] == 'key':
        k = count()
        return lambda : k.__next__()
    else:
        return lambda : int(random.uniform(0, 32000))

def textual_data_generator(column) -> Callable:
    try:
        # If have no samples or no sato category...
        if  'metadata' in column and (column['metadata'] and \
            (not 'samples' in column['metadata'] or not column['metadata']['samples'])) \
            and not ('satoCategory' in column['metadata'] and column['metadata']['satoCategory']):
            print("First case textual data generator ")
            if column['type'] == 'key':
                return lambda : str(uuid.uuid4())
            elif 'distinct' in column['metadata'] and column['metadata']['distinct'] == 0:
                return lambda : ''
            else:
                raise Exception(f'Bad textual generator for column {column["name"]}')
        else:
            if not column['metadata'] and column['type'] == 'key':
                return lambda : str(uuid.uuid4())

            generator = type78_generator(column['metadata']['satoCategory'])
            if not generator:
                print(f"No text generator assigned for satoCategory {column['metadata']['satoCategory']}")
            else:
                return generator

            if 'samples' in column['metadata']:
                samples = column['metadata']['samples']
                return lambda : random.choice(samples)

            # No samples and no generator
            print(f"[WARN] No samples available for textual column {column['name']}. Empty strings will be returned.")
            if column['type'] == 'key':
                lambda : str(uuid.uuid4())
            else:
                return lambda : ''
        if not column['metadata'] and column['type'] == 'key':
            return lambda : str(uuid.uuid4())
    except:
        print(f"[ERROR] Bad column data @ {column}")
        raise

def date_generator(column) -> Callable:
    if 'metadata' in column:
        max = datetime.now().timestamp()
        min = datetime.now().timestamp()
        if 'max' in column['metadata']:
            max = parse_date(column['metadata']['max'])
        if 'min' in column['metadata']:
            min = parse_date(column['metadata']['min'])
        if min > max:
            min, max = max, min
        return lambda : random.uniform(min, max)
    else:
        return lambda : datetime.now()

# compare type(column.type) with the actual types
generators_per_native_type = {
    'SMALLINT' : small_int_generator,
    'VARCHAR' : textual_data_generator,
    'TEXT' : textual_data_generator,
    'CHAR' : textual_data_generator,
    'CHARACTER VARYING' : textual_data_generator,
    'CHARACTER' : textual_data_generator,
    'INTEGER' : small_int_generator,
    'REAL' : small_int_generator,
    'BIGINT' : small_int_generator,
    'BOOLEAN' : boolean_generator,
    'BYTEA' : null_generator,
    'UUID' : uuid_generator,
    'DATE' :  date_generator,
    'TIMESTAMP' : date_generator
}

def get_row_generators(table_spec, datasets):
    row_generators = {}

    #print(table_spec['constraints']['foreign_key'])
    columns_with_fks = { dep['column'] : dep for dep in table_spec['constraints']['foreign_key'] }

    for column in table_spec['columns']:
        column_type = column['nativeType'].upper()
        if column['name'] in columns_with_fks:
            dependency = columns_with_fks[column['name']]

            # Define an entry in datasets if the column is in another table
            source_column = (dependency['refer_schema'], dependency['refer_table'], dependency['refer_column'])
            datasets[source_column] = []

            def generate_fk_data():
                if not datasets[source_column]:
                    print(f"[WARN] No data available in {source_column} for filling foreign key bounded column {column['name']} ({dependency['name']})")
                    return None
                else:
                    return random.choice(datasets[source_column])

            row_generators[column['name']] = generate_fk_data

        elif column_type in generators_per_native_type:
            print(f"Using native type based generator for column {column['name']} @ {table_spec['name']}")
            row_generators[column['name']] = generators_per_native_type[column_type](column)
        else:
            print(f"[WARN] Column {column['name']} with native database type {column_type} has no generator defined and will not receive any data")
            row_generators[column['name']] = null_generator(column)
    return row_generators