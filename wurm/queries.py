from dataclasses import dataclass, fields
from typing import Any

from . import sql
from .connection import execute, ensure_created, WurmError
from .typemaps import from_stored, to_stored

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

def decode_row(table, row):
    rowid, *py_values = (
        from_stored(field.type, stored_value)
        for stored_value, field
        in zip(row, fields(table))
        )
    item = table(*py_values)
    item.rowid = rowid
    return item

class Query:
    def __init__(self, table, filters):
        self.table = table
        self.filters = {key: ensure_comparison(value) for key, value in filters.items()}
        self.comparisons = ' and '.join(f'{key}{value.op}?' for key, value in self.filters.items())
        self.values = tuple(encode_query_value(table, key, value.value) for key, value in self.filters.items())
    def __len__(self):
        ensure_created(self.table)
        c, = execute(sql.count(self.table, self.comparisons), self.values).fetchone()
        return c
    def select_with_limit(self, limit=None):
        ensure_created(self.table)
        if limit is not None:
            values = self.values + (limit,)
        else:
            values = self.values
        for row in execute(sql.select(self.table, self.comparisons, limit is not None), values):
            yield decode_row(self.table, row)
    def __iter__(self):
        return self.select_with_limit()
    def _only_first(self, *, of):
        try:
            i, = self.select_with_limit(of)
        except ValueError as e:
            raise WurmError(e.args[0].replace('values to unpack', 'rows returned')) from None
        return i
    def first(self):
        return self._only_first(of=1)
    def one(self):
        return self._only_first(of=2)
    def delete(self):
        execute(sql.delete(self.table, self.comparisons), self.values)
