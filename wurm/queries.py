from dataclasses import dataclass, fields
from typing import Any

from . import sql
from .connection import execute, ensure_created, WurmError
from .typemaps import to_stored

@dataclass(frozen=True)
class Comparison:
    op: str
    value: Any

def gt(value):
    return Comparison('>', value)

def lt(value):
    return Comparison('<', value)

def ge(value):
    return Comparison('>=', value)

def le(value):
    return Comparison('<=', value)

def eq(value):
    return Comparison('=', value)

def ne(value):
    return Comparison('!=', value)

def ensure_comparison(value):
    if isinstance(value, Comparison):
        return value
    return eq(value)

def encode_query_value(table, fieldname, value):
    for field in fields(table):
        if field.name == fieldname:
            return to_stored(field.type, value)
    raise WurmError(f'invalid query: {table.__name__}.{fieldname} does not exist')

class Query:
    def __init__(self, table, filters):
        self.table = table
        self.filters = {key: ensure_comparison(value) for key, value in filters.items()}
        self.comparisons = ' and '.join(f'{key}{value.op}?' for key, value in self.filters.items())
        self.values = tuple(encode_query_value(table, key, value.value) for key, value in self.filters.items())
    def __len__(self):
        ensure_created(self.table)
        c, = execute(sql.count(self.table, self.comparisons if self.comparisons else None), self.values).fetchone()
        return c
    def __iter__(self):
        raise NotImplementedError  # noqa
    def first(self):
        raise NotImplementedError  # noqa
    def one(self):
        raise NotImplementedError  # noqa
    def delete(self):
        raise NotImplementedError  # noqa
