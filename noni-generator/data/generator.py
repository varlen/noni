import random
from typing import Callable
from sqlalchemy.dialects import postgresql as pgtypes
from data.textual import type78_generator

def small_int_generator(column) -> Callable:
    return lambda : int(random.uniform(0, 32000))

def textual_data_generator(column) -> Callable:
    if column.metadata and not column.metadata.columnData.samples:
        return lambda : None
    else:
        return type78_generator(
            column.metadata.colunmData.satoCategory,
            column.metadata.columnData.samples
        )

# compare type(column.type) with the actual types
generator_bindings = {
    pgtypes.SMALLINT : small_int_generator,
    pgtypes.VARCHAR : textual_data_generator,
    pgtypes.TEXT : textual_data_generator,
    # TODO -> Add bindings for pgtypes
}

def get_row_generators(table_spec):
    row_generators = {}
    for column in table_spec.columns:
        column_type = type(column.type)
        if column_type in generator_bindings:
            row_generators[column.name] = generator_bindings[column_type](column)
        else:
            print(f"Column with native database type {column_type} has no generator defined and will not receive any data")
    return row_generators