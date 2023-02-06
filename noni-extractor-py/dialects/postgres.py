import re
from collections import Counter
import requests
from rich import print

DEFAULT_SATO_MODEL_URL = "http://localhost:5000/upload-and-predict"

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

KEYS_QUERY = \
"""SELECT t.table_schema, t.table_name, c.column_name,
ccu.constraint_name, tc.constraint_type
FROM information_schema.tables as t
inner join information_schema.columns as c
    on t.table_name = c.table_name
    and t.table_catalog = c.table_catalog
    and t.table_schema = c.table_schema
inner join information_schema.constraint_column_usage as ccu
    on c.column_name = ccu.column_name
    and c.table_name = ccu.table_name
    and c.table_schema = ccu.table_schema
inner join information_schema.table_constraints as tc
    on ccu.constraint_name = tc.constraint_name
    and ccu.constraint_schema = tc.constraint_schema
    and ccu.table_name = tc.table_name
where t.table_schema not in ('pg_catalog', 'information_schema')
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
    return f"""SELECT DISTINCT count({column_name}) FROM {table_schema}.{table_name}"""

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


def get_table(structure, name, schema = 'public'):
    key = f'{schema}.{name}'
    if not key in structure:
        structure[key] = {
            'name' : name,
            'schema' : schema,
            'columns' : []
        }
    return structure[key]

def get_tables_columns_dataset(engine, db):
    return db.get(engine, TABLES_COLUMNS_QUERY)

def get_keys_dataset(engine, db):
    return db.get(engine, KEYS_QUERY)

def infer_agnostic_column_type(native_type, column_name):
    time_types = { 'date', 'timestamp without time zone', 'timestamp with timezone', 'time without time zone', 'time with time zone' }
    text_types = { 'text', 'character varying', 'character', 'citext', 'json', 'jsonb', 'xml' }
    integer_types = { 'smallint', 'integer', 'bigint' }
    decimal_types = { 'real', 'double precision', 'numeric', 'money' }
    boolean_types = { 'bit(1)', 'bool' }

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

def get_database_structure(engine, db):

    # Load tables_columns data
    tables_columns_dataset = get_tables_columns_dataset(engine, db)

    # Load database keys information
    keys_dataset = get_keys_dataset(engine, db)

    # TODO - Add key information as metadata

    tables_dict = {}

    # t.table_schema, t.table_name, t.table_type, c.column_name, c.ordinal_position, 
    # c.column_default, c.is_nullable, c.data_type, c.character_maximum_length, c.numeric_precision

    for column_data in tables_columns_dataset:
        schema_name, table_name, table_type,\
            column_name, ordinal_position, \
            column_default, is_nullable, native_type,\
            max_char_length, num_precision = column_data
        print(f'{schema_name}.{table_name}.{column_name} [{native_type}]')
        table_spec = get_table(tables_dict, table_name, schema_name)
        column_definition = {
            'name' : column_name,
            'nativeType' : native_type,
            'type' : infer_agnostic_column_type(native_type, column_name),
            'foreignKey' : None
        }
        table_spec['columns'].append(column_definition)

    database_structure = {
        'tables' : [ v for v in tables_dict.values()]
    }
    return database_structure

##### Named entity matching

named_entity_regexps = { k : re.compile(v) for k,v in {
    'CPF' : '(^\d{10,11}$)|(^\d{3}\.\d{3}\.\d{3}-\d{2}$)',
    'CEP' : '^(\d{8}|\d{2}\.?\d{3}\-\d{3})$',
    'CreditCardNumber' : '^(\d{4} ?){4}$',
    'CNPJ' : '^(\d{14}|\d{2}\.?\d{3}\.?\d{3}\/?\d{4}\-?\d{2})$',
    'LicensePlate' : '(^([a-z]|[A-Z]){3}-?\d{4}$)|(^([a-z]|[A-Z]){3}\d([a-z]|[A-Z])\d{2}$)',
    'ID' : '^\d{2}.?\d{3}.?\d{3}-?\d$',
    'Name' : '^(\w+ ?)+$',
    'Email' : '^[a-z0-9.]+@[a-z0-9]+\.[a-z]+\.([a-z]+)?$',
    'UUID' : '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$',
    'PhoneNumber' : '^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$',
    'PIS' : '^\d{11}$',
    'Address' : '^(\w+ ?)+.+\d+$'
}.items()}

def test_sample_for_named_entity(sample):
    for entity_name, regex in named_entity_regexps.items():
        if regex.fullmatch(sample):
            return entity_name.lower()
    return 'unknown'

def match_entity(samples):
    if not len(samples):
        return 'unknown'
    votes = Counter([ test_sample_for_named_entity(s) for s in samples ])
    return votes.most_common(1)[0][0]

def try_categories_from_samples(samples):
    categorizer = Counter(samples)
    return [ category for category, _ in categorizer.most_common() ]

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

def get_text_column_metadata(engine, db, column, table):
    m = get_default_column_metadata(engine, db, column, table)

    count_query = get_count_query(table['schema'], table['name'], column['name'])
    count = db.get(engine, count_query)
    row_count = 0
    if len(count):
        row_count = count[0][0]

    m['rowCount'] = row_count
    if column['nativeType'] == 'text':
        return m
    samples = []
    if row_count:
        rows_to_sample = 100
        m['sampleCount'] = rows_to_sample
        proportion = max(min(100, (100 * (rows_to_sample / row_count))), 1)

        data_sample_query = get_datasample_query(table['schema'], table['name'], column['name'], proportion, rows_to_sample)
        data_sample = db.get(engine, data_sample_query)
        m['sampleCount'] = len(data_sample)
        samples = [ s[0] for s in data_sample ]
        m['samples'] = samples

        print("  Testing textual category")
        m['entityType'] = match_entity(samples)

    if not m['entityType'] or m['entityType'] == 'unknown':
        print(f"  Could not identify textual data type. Examples: \n    {'\n    '.join(samples[0:3])}")

        possible_categories = try_categories_from_samples(samples)
        print(possible_categories)
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

def csv_escape(txt):
    return str(txt).replace(',','.').replace('\n','\t')

def generate_csv_from_table(engine, db, table, max_rows = 50):
    column_names = [ column['name'] for column in table['columns']]
    select_rows_query = get_select_rows_query(column_names, table['schema'], table['name'], max_rows)
    loaded_rows = db.get(engine, select_rows_query)
    if not len(loaded_rows):
        print(f"[red]  No csv sample for table '{table['name']}'[/red]")
        return None

    csv_rows = [','.join(column_names)]
    for row in loaded_rows:
        csv_rows.append(','.join([ csv_escape(value) for value in row ]))
    print(f"  CSV sample for table '{table['name']}' ready")
    return '\n'.join(csv_rows)

def run_semantic_inference_model(table, csv_str, model_url = DEFAULT_SATO_MODEL_URL):
    table_name = table['name']
    column_names = [ c['name'] for c in table['columns']]
    file_name = table_name.lower() + '.csv'
    files = { 'file': (file_name, csv_str)}
    inferences = requests.post(model_url, files=files).json()
    return dict(zip(column_names, inferences))

def add_metadata(engine, db, structure):
    # NOTE - this whole semantic inference process is database agnostic and should not be in a specific dialect implementation
    for table in structure['tables']:
        for column in table['columns']:
            column['metadata'] = get_column_metadata(engine, db, column, table)

        # generate csv from table sample
        csv_table_sample = generate_csv_from_table(engine, db, table)
        if csv_table_sample:
            # call SATO for semantic inference
            semantic_inferences = run_semantic_inference_model(table, csv_table_sample)
            print(f"[purple]  Semantic inferences: {semantic_inferences}[/purple]")

            # map semantic inferences to generators
            for column in table['columns']:
                if column['name'] in semantic_inferences:
                    inferred_category = semantic_inferences[column['name']]
                    print(column)
                    if not 'metadata' in column or not column['metadata']:
                        column['metadata'] = {}
                    column['metadata']['satoCategory'] = inferred_category

                # TODO - Depending on the category, it may no longer be a text column

    return structure
