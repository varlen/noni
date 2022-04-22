import random, uuid
from typing import Callable
from datetime import datetime
from itertools import count
from data.textual import type78_generator

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
        if column['metadata'] and \
            (not column['metadata']['columnData']
                or not 'samples' in column['metadata']['columnData']) or not column['metadata']:
            print("First case textual data generator ")
            if column['type'] == 'key':
                lambda : str(uuid.uuid4())
            else:
                return lambda : None
        else:
            generator = type78_generator(column['metadata']['columnData']['satoCategory'])
            if generator:
                return generator

            samples = column['metadata']['columnData']['samples']
            if samples:
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
    # TODO - Improve to generate dates within the samples range
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

def get_row_generators(table_spec):
    row_generators = {}
    for column in table_spec['columns']:
        column_type = column['nativeType'].upper()
        if column_type in generators_per_native_type:
            print(f"Using native type based generator for column {column['name']} @ {table_spec['name']}")
            row_generators[column['name']] = generators_per_native_type[column_type](column)
        else:
            print(f"[WARN] Column {column['name']} with native database type {column_type} has no generator defined and will not receive any data")
            row_generators[column['name']] = null_generator(column)
    return row_generators