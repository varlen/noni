import random
from sqlalchemy.dialects import postgresql as pgtypes
from data.textual import type78_generator

def small_int_generator(column):
    return int(random.uniform(0, 32000))

def textual_data_generator(column):
    if column.metadata and not column.metadata.columnData.samples:
        return None
    else:
        return type78_generator(column.metadata.colunmData.satoCategory)

# compare type(column.type) with the actual types
generator_bindings = {
    pgtypes.SMALLINT : small_int_generator,
    pgtypes.VARCHAR : textual_data_generator,
    pgtypes.TEXT : textual_data_generator,
}

# OBSOLETE
def create_data_for_table(table_model):

    row_data = {}
    for column in table_model.columns:
        column_type = type(column.type)
        if column_type in generator_bindings:
            row_data[column.name] = generator_bindings[column_type](column)
        else:
            print(f"Column type {column_type} has no generator defined")

    pass

def create_data_for_table_spec(spec, number_of_rows):
    dataset = []
    for _ in range(number_of_rows):
        row_data = {}
        for column in spec['columns']:
            pass