import requests
from rich import print
from analysis import numerictools, semantics, named_entity

TABLES_COLUMNS_QUERY = \
"""SELECT t.table_schema, t.table_name, t.table_type, c.column_name, c.ordinal_position,
c.column_default, c.is_nullable, c.data_type, c.character_maximum_length, c.numeric_precision
FROM information_schema.tables as t
inner join information_schema.columns as c
on t.table_name = c.table_name
and t.table_catalog = c.table_catalog
and t.table_schema = c.table_schema
where t.table_schema not in ('pg_catalog', 'information_schema')
and t.table_type = 'BASE TABLE';"""

CONSTRAINTS_QUERY = \
"""SELECT t.table_schema, t.table_name, c.column_name,
ccu.constraint_name, tc.constraint_type, kcu.table_schema,
kcu.table_name, kcu.column_name, kcu.ordinal_position, kcu.position_in_unique_constraint
FROM information_schema.tables as t
left join information_schema.columns as c
    on t.table_name = c.table_name
    and t.table_catalog = c.table_catalog
    and t.table_schema = c.table_schema
left join information_schema.constraint_column_usage as ccu
    on c.column_name = ccu.column_name
    and c.table_name = ccu.table_name
    and c.table_schema = ccu.table_schema
left join information_schema.key_column_usage as kcu
	on kcu.constraint_name = ccu.constraint_name
	and kcu.constraint_schema = ccu.constraint_schema
left join information_schema.table_constraints as tc
    on ccu.constraint_name = tc.constraint_name
    and ccu.constraint_schema = tc.constraint_schema
    and ccu.table_name = tc.table_name
where t.table_schema not in ('pg_catalog', 'information_schema')
and ccu.constraint_name is not NULL
and t.table_type = 'BASE TABLE';"""

def get_numeric_stat_query(table_schema, table_name, column_name):
    return f"""SELECT avg({column_name}) as average,
max({column_name}) as max,
min({column_name}) as min,
variance({column_name}) as variance
FROM {table_schema}.{table_name}
"""

def get_numeric_mode_query(table_schema, table_name, column_name):
    return f"""SELECT mode() WITHIN GROUP (ORDER BY {column_name}) As modal FROM {table_schema}.{table_name}"""

def get_maxmin_query(table_schema, table_name, column_name):
    return f"""SELECT
max({column_name}) as max,
min({column_name}) as min
FROM {table_schema}.{table_name}"""

def get_bool_count_query(table_schema, table_name, column_name):
    return f"""SELECT {column_name}::text, count(1) FROM {table_schema}.{table_name} group by {column_name}"""

def get_distinct_count_query(table_schema, table_name, column_name):
    return f"""SELECT count(1) FROM (SELECT DISTINCT {column_name} FROM {table_schema}.{table_name}) AS tmp"""

def get_count_query(table_schema, table_name, column_name):
    return f"""SELECT count({column_name}) as count FROM {table_schema}.{table_name}"""

def get_datasample_query(table_schema, table_name, column_name, proportion, rows_to_sample):
    'Requires Postgres 9.5+'
    return f"""SELECT DISTINCT {column_name} FROM {table_schema}.{table_name}
TABLESAMPLE SYSTEM ({proportion})
WHERE {column_name} IS NOT NULL LIMIT {rows_to_sample}"""

def get_select_rows_query(column_names, table_schema, table_name, max_rows):
    return f"""SELECT {', '.join(column_names)} FROM {table_schema}.{table_name} LIMIT {max_rows};"""
### End of queries

# NOT POSTGRES SPECIFIC
def get_table(structure, name, schema = 'public'):
    key = f'{schema}.{name}'
    if not key in structure:
        structure[key] = {
            'name' : name,
            'schema' : schema,
            'columns' : []
        }
    return structure[key]

# NOT POSTGRES SPECIFIC
def get_tables_columns_dataset(engine, db):
    return db.get(engine, TABLES_COLUMNS_QUERY)

# NOT POSTGRES_SPECIFIC
def get_keys_dataset(engine, db):
    return db.get(engine, CONSTRAINTS_QUERY)

# types are postgres specific... Maybe split into a dict + actual non-specific function
def infer_agnostic_column_type(native_type, column_name):
    print(f"[red] INFERING COLUMN TYPE FOR {native_type} {column_name} [/red]")
    time_types = { 'date', 'timestamp without time zone', 'timestamp with timezone', 'time without time zone', 'time with time zone' }
    text_types = { 'text', 'character varying', 'character', 'citext', 'json', 'jsonb', 'xml' }
    integer_types = { 'smallint', 'integer', 'bigint' }
    decimal_types = { 'real', 'double precision', 'numeric', 'money' }
    boolean_types = { 'bit(1)', 'bool', 'boolean' }

    if column_name.startswith('id_') or column_name.endswith('_id') or native_type == 'uuid':
        return 'key'
    if native_type in time_types:
        return 'time'
    if native_type in text_types:
        return 'text'
    if native_type in integer_types:
        return 'integer'
    if native_type in decimal_types:
        return 'decimal'
    if native_type in boolean_types:
        return 'bool'
    return 'unknown'

# Data model based on postgres but can be modular
# TODO - Add documentation providing expected output example
def get_constraints(engine, db):
    keys_dataset = get_keys_dataset(engine, db)

    keys_by_table = {}
    for key_info in keys_dataset:
        source_schema, source_table, source_column, constraint_name, constraint_type, \
            target_schema, target_table, target_column, \
            ordinal_position, unique_position = key_info
        if not (target_schema, target_table) in keys_by_table:
            keys_by_table[(target_schema, target_table)] = {
                'primary_key' : [],
                'foreign_key' : []
            }
        if constraint_type == 'PRIMARY KEY':
            keys_by_table[(target_schema, target_table)]['primary_key'].append({
                'name' : constraint_name,
                'column' : target_column,
                'order' : ordinal_position
            })
        else:
            keys_by_table[(target_schema, target_table)]['foreign_key'].append({
                'name' : constraint_name,
                'column' : target_column,
                'refer_schema' : source_schema,
                'refer_table' : source_table,
                'refer_column' : source_column,
                'order' : ordinal_position,
                'unique_order' : unique_position
            })
    return keys_by_table

# Core function of extractor. Maybe move it somewhere else and
# make it dialect agnostic?
# Definitelly not postgres exclusive
def get_database_structure(engine, db):

    # Load tables_columns data
    tables_columns_dataset = get_tables_columns_dataset(engine, db)

    # Load database keys information
    constraints = get_constraints(engine, db)

    tables_dict = {}

    for column_data in tables_columns_dataset:
        schema_name, table_name, table_type,\
            column_name, ordinal_position, \
            column_default, is_nullable, native_type,\
            max_char_length, num_precision = column_data
        print(f'{schema_name}.{table_name}.{column_name} [{native_type}]')
        table_spec = get_table(tables_dict, table_name, schema_name)
        ag_col_type = infer_agnostic_column_type(native_type, column_name)
        print(f"[red]   {ag_col_type}[/red]")
        column_definition = {
            'name' : column_name,
            'nativeType' : native_type,
            'type' : ag_col_type,
            'properties' : {
                'nullable' : bool(is_nullable),
            }
        }

        if num_precision:
            column_definition['properties']['precision'] = num_precision

        if max_char_length:
            column_definition['properties']['maxLength'] = max_char_length

        table_spec['columns'].append(column_definition)
        if not 'constraints' in table_spec and (schema_name, table_name) in constraints:
            table_spec['constraints'] = constraints[(schema_name, table_name)]

    database_structure = {
        'tables' : [ v for v in tables_dict.values()]
    }

    return database_structure

##### Named entity matching

def get_row_count(engine, db, column, table):
    count_query = get_count_query(table['schema'], table['name'], column['name'])
    count = db.get(engine, count_query)
    row_count = 0
    if len(count):
        row_count = count[0][0]
    return row_count

##### Column metadata collectors

def get_default_column_metadata(engine, db, column, table):
    m = {}
    distinct_query = get_distinct_count_query(table['schema'], table['name'], column['name'])
    distinct_count = db.get(engine, distinct_query)
    if len(distinct_query):
        m['distinct'] = distinct_count[0][0]
    return m

def get_numeric_column_metadata(engine, db, column, table):
    m = get_default_column_metadata(engine, db, column, table)
    numeric_stat_query = \
        get_numeric_stat_query(table['schema'], table['name'], column['name'])
    numeric_stat = db.get(engine, numeric_stat_query)
    if len(numeric_stat):
        m['average'], m['max'], m['min'], m['variance'] = numeric_stat[0]
    else:
        print(f'[yellow] No numeric stats metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
        return m

    mode_query = \
        get_numeric_mode_query(table['schema'], table['name'], column['name'])
    mode_result = db.get(engine, mode_query)

    if len(mode_result):
        m['mode'] = mode_result[0][0]
    else:
        print(f'[yellow] No numeric mode metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')

    # distribution inference
    if m['distinct'] > 1 and 'metadata' in table and 'rowCount' in table['metadata'] :
        print("[green]  Sampling numeric distribution[/green]")
        row_count = m['distinct']
        if row_count < 1000:
            rows_to_sample = row_count
        else:
            rows_to_sample = 1000

        column_numeric_samples = get_column_samples(engine, db, column, table, row_count , rows_to_sample)
        SAMPLES_FOR_NUMERIC_COLUMN = 40
        similar_sequence = numerictools.sequence_from_samples(column_numeric_samples, SAMPLES_FOR_NUMERIC_COLUMN, unique=True)
        m['sequence'] = similar_sequence

    return m

def get_boolean_column_metadata(engine, db, column, table):
    m = get_default_column_metadata(engine, db, column, table)
    bool_query = get_bool_count_query(table['schema'], table['name'], column['name'])
    bool_stats = db.get(engine, bool_query)
    m['entityType'] = 'boolean'
    m['categories'] = {}
    for boolean, count in bool_stats:
        m['categories'][boolean] = count
    return m

def get_datetime_column_metadata(engine, db, column, table):
    m = get_default_column_metadata(engine, db, column, table)
    max_min_query = \
        get_maxmin_query(table['schema'], table['name'], column['name'])

    max_min = db.get(engine, max_min_query)
    if len(max_min):
        m['max'], m['min'] = max_min[0]
    else:
        print(f'[yellow] No start and end date metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
        return m

    mode_query = \
        get_numeric_mode_query(table['schema'], table['name'], column['name'])

    mode_result = db.get(engine, mode_query)
    if len(mode_result):
        m['mode'] = mode_result[0][0]
    else:
        print(f'[yellow] No date time mode metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
    return m

def get_column_samples(engine, db, column, table, row_count, rows_to_sample):
    proportion = max(min(100, (100 * (rows_to_sample / row_count))), 1)
    data_sample_query = get_datasample_query(table['schema'], table['name'], column['name'], proportion, rows_to_sample)
    data_sample = db.get(engine, data_sample_query)
    samples = [ s[0] for s in data_sample ]
    return samples

def get_text_column_metadata(engine, db, column, table):
    m = get_default_column_metadata(engine, db, column, table)

    row_count = get_row_count(engine, db, column, table) # refactor to count once
    m['rowCount'] = row_count
    if column['nativeType'] == 'text':
        return m
    samples = []
    if row_count:
        rows_to_sample = 100
        data_sample = get_column_samples(engine, db, column, table, row_count, rows_to_sample)
        m['sampleCount'] = len(data_sample)
        m['samples'] = samples
        m['entityType'] = named_entity.match_entity(samples)

    if not m['entityType'] or m['entityType'] == 'unknown':
        print(f"  Could not identify textual data type. Examples: {', '.join(samples[0:3])}")

        possible_categories = named_entity.try_categories_from_samples(samples)
        if len(possible_categories) == len(samples):
            print("  Data in column is probably not categoric. Using default metadata structure.")
            return m
        else:
            m['categories'] = possible_categories

    return m

column_metadata_collectors = {
    'key': lambda x,y,z,w : None,
    'time': get_datetime_column_metadata,
    'text': get_text_column_metadata,
    'integer': get_numeric_column_metadata,
    'decimal': get_numeric_column_metadata,
    'bool': get_boolean_column_metadata
}

##### End of column metadata collectors

def get_column_metadata(engine, db, column, table):
    agnostic_column_type = column['type']
    if agnostic_column_type in column_metadata_collectors:
        return column_metadata_collectors[agnostic_column_type](engine, db,column, table)
    else:
        return get_default_column_metadata(engine, db, column, table)


def add_metadata(engine, db, structure):
    # TODO - Refactor out. This whole semantic inference process is database agnostic and should not be in a specific dialect implementation
    for table in structure['tables']:

        row_count = get_row_count(engine, db, {'name' : 1}, table) # TODO - Improve this ugly call
        table['metadata'] = {
            'rowCount' : row_count
        }

        for column in table['columns']:
            column['metadata'] = get_column_metadata(engine, db, column, table)

        semantics.add_semantic_metadata_to_table(engine, db, table)
        # TODO - Depending on the category, it may no longer be a text column

    return structure
