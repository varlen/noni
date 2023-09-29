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

type_mapping = {
    'time' : { 'date', 'timestamp without time zone', 'timestamp with timezone', 'time without time zone', 'time with time zone' },
    'text' : { 'text', 'character varying', 'character', 'citext', 'json', 'jsonb', 'xml', 'USER-DEFINED' },
    'integer' : { 'smallint', 'integer', 'bigint' },
    'decimal' : { 'real', 'double precision', 'numeric', 'money' },
    'boolean' : { 'bit(1)', 'bool', 'boolean' }
}

def get_column_samples(engine, db, column, table, row_count, rows_to_sample):
    proportion = max(min(100, (100 * (rows_to_sample / row_count))), 1)
    data_sample_query = get_datasample_query(table['schema'], table['name'], column['name'], proportion, rows_to_sample)
    data_sample = db.get(engine, data_sample_query)
    samples = [ s[0] for s in data_sample ]
    return samples
