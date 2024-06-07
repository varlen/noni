import random, uuid
from typing import Callable
from datetime import datetime
from itertools import count
from data.textual import type78_generator
from dateutil.parser import parse as parse_date
from rich import print
from data.numeric import next_from_distribution

def null_generator(column) -> Callable:
    if column['type'] == 'key':
        raise Exception('Cannot use NULL generator for key column')
    return lambda : None

def uuid_generator(column) -> Callable:
    return lambda : uuid.uuid4()

def boolean_generator(column) -> Callable:
    return lambda : random.choice([True, False])

def generic_distribution_generator(column) -> Callable:
    is_integer = column['type'] == 'integer'

    if 'metadata' in column and column['metadata'] and 'sequence' in column['metadata']:
        previous_draws_set = set()
        previous_draws_list = []

        def draw_from_distribution():
            previous_draws_list
            draw = next_from_distribution(column['metadata']['sequence'], is_integer)
            if len(previous_draws_set) < column['metadata']['distinct']:
                previous_draws_set.add(draw)
                if len(previous_draws_set) == column['metadata']['distinct']:
                    previous_draws_list.extend(list(previous_draws_set))
                return draw
            else:
                return random.choice(previous_draws_list)

        return draw_from_distribution
    else:
        return uniform_number_generator(column)

def uniform_number_generator(column) -> Callable:
    if column['type'] == 'key':
        k = count()
        return lambda : k.__next__()
    else:
        min = 1
        max = 32000
        if 'metadata' in column:
            if 'max' in column['metadata'] and column['metadata']['max']:
                max = float(column['metadata']['max'])
            if 'min' in column['metadata'] and column['metadata']['min']:
                min = float(column['metadata']['min'])

        if column['type'] == 'integer':
            return lambda : round(random.uniform(min, max))
        else:
            return lambda : random.uniform(min, max)

def textual_data_generator(column) -> Callable:
    try:
        # If have no samples or no sato category...
        if  'metadata' in column and (column['metadata'] and \
            (not 'samples' in column['metadata'] or not column['metadata']['samples'])) \
            and not ('semanticClass' in column['metadata'] and column['metadata']['semanticClass']):
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

            generator = type78_generator(column['metadata']['semanticClass'])
            if not generator:
                print(f"[yellow][WARN] No text generator assigned for semanticClass {column['metadata']['semanticClass']}[/yellow]")
            else:
                return generator

            if 'samples' in column['metadata'] and len(column['metadata']['samples']):
                samples = column['metadata']['samples']
                return lambda : random.choice(samples)

            # No samples and no generator
            print(f"[yellow][WARN] No samples available for textual column {column['name']}. Empty strings will be returned.[/yellow]")
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
        max = datetime.now()
        min = datetime.now()
        if 'max' in column['metadata'] and column['metadata']['max']:
            max = parse_date(column['metadata']['max'])
        if 'min' in column['metadata'] and column['metadata']['min']:
            min = parse_date(column['metadata']['min'])
        if min > max:
            min, max = max, min
        print(f"DATE GENERATOR SPEC - {column['name']}: {type(min)} {min} - {type(max)} {max}")
        return lambda : datetime.fromtimestamp(random.uniform(min.timestamp(), max.timestamp()))
    else:
        return lambda : datetime.now()

# compare type(column.type) with the actual types
generators_per_native_type = {
    'SMALLINT' : uniform_number_generator,
    'VARCHAR' : textual_data_generator,
    'TEXT' : textual_data_generator,
    'CHAR' : textual_data_generator,
    'CHARACTER VARYING' : textual_data_generator,
    'CHARACTER' : textual_data_generator,
    'TSVECTOR' : textual_data_generator,
    'USER-DEFINED' : textual_data_generator,
    'INTEGER' : uniform_number_generator,
    'NUMERIC' : uniform_number_generator,
    'REAL' : uniform_number_generator,
    'BIGINT' : uniform_number_generator,
    'BOOLEAN' : boolean_generator,
    'BYTEA' : null_generator,
    'UUID' : uuid_generator,
    'DATE' :  date_generator,
    'TIMESTAMP' : date_generator,
    'TIMESTAMP WITHOUT TIME ZONE' : date_generator
}

def make_fk_data_generator(source_column, datasets, column_name, dep_name):
    print(f"[yellow] Building generator for {source_column}")
    def generate_fk_data():
        if not datasets[source_column]:
            print(f"[red] No data available in {source_column} for filling foreign key bounded column {column_name} ({dep_name})")
            return None
        else:
            chosen_key = random.choice(datasets[source_column])
            print(f"[cyan] Generating data for FK in {column_name} taking from {source_column} -> {chosen_key}")
            return chosen_key

    return generate_fk_data

def get_row_generators(table_spec, datasets):
    row_generators = {}

    #print(table_spec['constraints']['foreign_key'])
    columns_with_fks = { dep['column'] : dep for dep in table_spec['constraints']['foreign_key'] }
    print(f"COLUMNS WITH FK: {columns_with_fks}")
    print(f"[yellow] FKs in table {table_spec['name']}: {columns_with_fks}")

    for column in table_spec['columns']:
        print(f"Defining generator for {table_spec['name']}.{column['name']}")
        column_type = column['nativeType'].upper()
        if column['name'] in columns_with_fks.keys():
            dependency = columns_with_fks[column['name']]
            # Define an entry in datasets if the column is in another table
            source_column = (dependency['refer_schema'], dependency['refer_table'], dependency['refer_column'])
            print(f"[pink]Column {column['name']} refers an FK:", source_column)
            if not source_column in datasets:
                datasets[source_column] = []

            row_generators[column['name']] = make_fk_data_generator(source_column, datasets, column['name'], dependency['name'])

        elif column_type in generators_per_native_type:
            print(f"Using native type based generator for column {column['name']} @ {table_spec['name']}")
            row_generators[column['name']] = generators_per_native_type[column_type](column)
        else:
            print(f"[yellow][WARN] Column {column['name']} with native database type {column_type} has no generator defined and will not receive any data[/yellow]")
            row_generators[column['name']] = null_generator(column)
    return row_generators