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
    """Helper function for selecting fields whose value is greater than
    *value*

    ``MyTable.query(myfield=gt(7))`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield > 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('>', value)

def lt(value):
    """Helper function for selecting fields whose value is lesser than
    *value*

    ``MyTable.query(myfield=lt(7))`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield < 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('<', value)

def ge(value):
    """Helper function for selecting fields whose value is greater than
    or equal to *value*

    ``MyTable.query(myfield=ge(7))`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield >= 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('>=', value)

def le(value):
    """Helper function for selecting fields whose value is lesser than
    or equal to *value*

    ``MyTable.query(myfield=le(7))`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield <= 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('<=', value)

def eq(value):
    """Helper function for selecting fields whose value is equal to
    *value*

    ``MyTable.query(myfield=7)`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield = 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('=', value)

def ne(value):
    """Helper function for selecting fields whose value is not equal to
    *value*

    ``MyTable.query(myfield=ne(7))`` roughly translates to
    ``SELECT * FROM MyTable WHERE myfield != 7``

    :param Any value: value being compared to the field
    :returns: object used in queries for comparison"""
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
        """Instantiate a Query object.

        Query(table, filters) == table.query(**filters)"""
        self.table = table
        self.filters = {key: ensure_comparison(value) for key, value in filters.items()}
        self.comparisons = ' and '.join(f'{key}{value.op}?' for key, value in self.filters.items())
        self.values = tuple(encode_query_value(table, key, value.value) for key, value in self.filters.items())
    def __len__(self):
        """Returns the number of rows matching this query.

        .. note:: This method accesses the connected database.

        :returns: number of matches
        :rtype: int
        """
        ensure_created(self.table)
        c, = execute(sql.count(self.table, self.comparisons), self.values).fetchone()
        return c
    def select_with_limit(self, limit=None):
        """Create an iterator over the results of this query.

        This accesses the database.

        :param limit: The number of results to limit this query to.
        :type limit: int or None
        :returns: an iterator over the objects matching this query."""
        ensure_created(self.table)
        if limit is not None:
            values = self.values + (limit,)
        else:
            values = self.values
        for row in execute(sql.select(self.table, self.comparisons, limit is not None), values):
            yield decode_row(self.table, row)
    def __iter__(self):
        """Iterate over the results of this query.

        .. note:: This method accesses the connected database.

        Equivalent to :meth:`select_with_limit` without specifying *limit*."""
        return self.select_with_limit()
    def _only_first(self, *, of):
        try:
            i, = self.select_with_limit(of)
        except ValueError as e:
            raise WurmError(e.args[0].replace('values to unpack', 'rows returned')) from None
        return i
    def first(self):
        """Return the first result of this query.

        .. note:: This method accesses the connected database.

        :raises WurmError: if this query returns zero results"""
        return self._only_first(of=1)
    def one(self):
        """Return the only result of this query.

        .. note:: This method accesses the connected database.

        :raises WurmError: if this query returns zero results or more than one"""
        return self._only_first(of=2)
    def delete(self):
        """Delete the objects matching this query.

        .. warning:: Calling this on an empty query deletes all rows
           in the database

        .. note:: This method accesses the connected database.

        :returns: the number of rows deleted
        :rtype: int
        """
        execute(sql.delete(self.table, self.comparisons), self.values).rowcount
