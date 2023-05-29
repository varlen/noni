from rich import print

from analysis import numerictools, semantics, named_entity

def _get_table(structure, name, schema = 'public'):
    key = f'{schema}.{name}'
    if not key in structure:
        structure[key] = {
            'name' : name,
            'schema' : schema,
            'columns' : []
        }
    return structure[key]

def get_tables_columns_dataset(engine, db, plugin):
    return db.get(engine, plugin.TABLES_COLUMNS_QUERY)

def get_keys_dataset(engine, db, plugin):
    return db.get(engine, plugin.CONSTRAINTS_QUERY)

def infer_agnostic_column_type(native_type, column_name, plugin):
    print(f"[red] INFERING COLUMN TYPE FOR {native_type} {column_name} [/red]")

    if column_name.startswith('id_') or column_name.endswith('_id') or native_type == 'uuid':
        return 'key'
    if native_type in plugin.type_mapping['time']:
        return 'time'
    if native_type in plugin.type_mapping['text']:
        return 'text'
    if native_type in plugin.type_mapping['integer']:
        return 'integer'
    if native_type in plugin.type_mapping['decimal']:
        return 'decimal'
    if native_type in plugin.type_mapping['boolean']:
        return 'bool'
    return 'unknown'

def get_constraints(engine, db, plugin):
    # TODO - Document schema
    keys_dataset = get_keys_dataset(engine, db, plugin)

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

def get_database_structure(engine, db, plugin):

    # Load tables_columns data
    tables_columns_dataset = get_tables_columns_dataset(engine, db, plugin)

    # Load database keys information
    constraints = get_constraints(engine, db, plugin)

    tables_dict = {}

    for column_data in tables_columns_dataset:
        schema_name, table_name, table_type,\
            column_name, ordinal_position, \
            column_default, is_nullable, native_type,\
            max_char_length, num_precision = column_data
        print(f'{schema_name}.{table_name}.{column_name} [{native_type}]')
        table_spec = _get_table(tables_dict, table_name, schema_name)
        ag_col_type = infer_agnostic_column_type(native_type, column_name, plugin)
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

## Metadata extractors

def get_row_count(engine, db, column, table, plugin):
    count_query = plugin.get_count_query(table['schema'], table['name'], column['name'])
    count = db.get(engine, count_query)
    row_count = 0
    if len(count):
        row_count = count[0][0]
    return row_count

def get_default_column_metadata(engine, db, column, table, plugin):
    m = {}
    distinct_query = plugin.get_distinct_count_query(table['schema'], table['name'], column['name'])
    distinct_count = db.get(engine, distinct_query)
    if len(distinct_query):
        m['distinct'] = distinct_count[0][0]
    return m

def get_numeric_column_metadata(engine, db, column, table, plugin):
    m = get_default_column_metadata(engine, db, column, table, plugin)
    numeric_stat_query = \
        plugin.get_numeric_stat_query(table['schema'], table['name'], column['name'])
    numeric_stat = db.get(engine, numeric_stat_query)
    if len(numeric_stat):
        m['average'], m['max'], m['min'], m['variance'] = numeric_stat[0]
    else:
        print(f'[yellow] No numeric stats metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
        return m

    ## TODO - Split this mode part into a separate function
    mode_query = \
        plugin.get_numeric_mode_query(table['schema'], table['name'], column['name'])
    mode_result = db.get(engine, mode_query)

    if len(mode_result):
        m['mode'] = mode_result[0][0]
    else:
        print(f'[yellow] No numeric mode metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')

    # TODO - Split this distribution inference part into a separate function
    # distribution inference
    if m['distinct'] > 1 and 'metadata' in table and 'rowCount' in table['metadata'] :
        print("[green]  Sampling numeric distribution[/green]")
        row_count = m['distinct']
        if row_count < 1000:
            rows_to_sample = row_count
        else:
            rows_to_sample = 1000

        column_numeric_samples = plugin.get_column_samples(engine, db, column, table, row_count , rows_to_sample)
        SAMPLES_FOR_NUMERIC_COLUMN = 40
        similar_sequence = numerictools.sequence_from_samples(column_numeric_samples, SAMPLES_FOR_NUMERIC_COLUMN, unique=True)
        m['sequence'] = similar_sequence

    return m

def get_boolean_column_metadata(engine, db, column, table, plugin):
    m = get_default_column_metadata(engine, db, column, table, plugin)
    bool_query = plugin.get_bool_count_query(table['schema'], table['name'], column['name'])
    bool_stats = db.get(engine, bool_query)
    m['entityType'] = 'boolean'
    m['categories'] = {
        boolean : count
        for boolean, count
        in bool_stats
    }
    return m

def get_text_column_metadata(engine, db, column, table, plugin):
    m = get_default_column_metadata(engine, db, column, table, plugin)

    row_count = get_row_count(engine, db, column, table, plugin) # TODO - refactor to count once
    m['rowCount'] = row_count
    if column['nativeType'] == 'text':
        return m
    samples = []
    if row_count:
        rows_to_sample = 100
        data_sample = plugin.get_column_samples(engine, db, column, table, row_count, rows_to_sample)
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

def get_datetime_column_metadata(engine, db, column, table, plugin):
    m = get_default_column_metadata(engine, db, column, table, plugin)
    max_min_query = \
        plugin.get_maxmin_query(table['schema'], table['name'], column['name'])

    max_min = db.get(engine, max_min_query)
    if len(max_min):
        m['max'], m['min'] = max_min[0]
    else:
        print(f'[yellow] No start and end date metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
        return m

    mode_query = \
        plugin.get_numeric_mode_query(table['schema'], table['name'], column['name'])

    mode_result = db.get(engine, mode_query)
    if len(mode_result):
        m['mode'] = mode_result[0][0]
    else:
        print(f'[yellow] No date time mode metadata available for column {table["schema"]}.{table["name"]}.{column["name"]} [/yellow]')
    return m

column_metadata_collectors = {
    'key': lambda x,y,z,w,p : None,
    'time': get_datetime_column_metadata,
    'text': get_text_column_metadata,
    'integer': get_numeric_column_metadata,
    'decimal': get_numeric_column_metadata,
    'bool': get_boolean_column_metadata
}

def get_column_metadata(engine, db, column, table, plugin):
    agnostic_column_type = column['type']
    if agnostic_column_type in column_metadata_collectors:
        return column_metadata_collectors[agnostic_column_type](engine, db, column, table, plugin)
    else:
        return get_default_column_metadata(engine, db, column, table, plugin)

def add_metadata(engine, db, structure, plugin):
    # TODO - Refactor out. This whole semantic inference process is database agnostic and should not be in a specific dialect implementation
    for table in structure['tables']:

        row_count = get_row_count(engine, db, {'name' : 1}, table, plugin) # TODO - Improve this ugly call
        table['metadata'] = {
            'rowCount' : row_count
        }

        for column in table['columns']:
            column['metadata'] = get_column_metadata(engine, db, column, table, plugin)

        semantics.add_semantic_metadata_to_table(engine, db, table, plugin)
        # TODO - Depending on the category, it may no longer be a text column

    return structure