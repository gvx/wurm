from dataclasses import dataclass
from typing import Any, Generic, TypeVar, Type, Dict, Optional, Iterator

from . import sql
from .connection import execute, WurmError
from .typemaps import from_stored, to_stored, columns_for


@dataclass(frozen=True)
class Comparison:
    op: str
    value: Any


def gt(value):
    """Helper function for selecting fields whose value is greater than
    *value*

    :samp:`{table}.query({myfield}=gt(7))` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} > 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('>', value)


def lt(value):
    """Helper function for selecting fields whose value is lesser than
    *value*

    :samp:`{table}.query({myfield}=lt(7))` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} < 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('<', value)


def ge(value):
    """Helper function for selecting fields whose value is greater than
    or equal to *value*

    :samp:`{table}.query({myfield}=ge(7))` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} >= 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('>=', value)


def le(value):
    """Helper function for selecting fields whose value is lesser than
    or equal to *value*

    :samp:`{table}.query({myfield}=le(7))` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} <= 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('<=', value)


def eq(value):
    """Helper function for selecting fields whose value is equal to
    *value*

    :samp:`{table}.query({myfield}=7)` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} = 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('=', value)


def ne(value):
    """Helper function for selecting fields whose value is not equal to
    *value*

    :samp:`{table}.query({myfield}=ne(7))` roughly translates to
    :samp:`SELECT * FROM {table} WHERE {myfield} != 7`

    :param value: value being compared to the field
    :returns: object used in queries for comparison"""
    return Comparison('!=', value)


def ensure_comparison(value):
    if isinstance(value, Comparison):
        return value
    return eq(value)


def encode_query_value(table, fieldname, value):
    for field, ty in table.__fields_info__.items():
        if field == fieldname:
            return to_stored(fieldname, ty, value)
    raise WurmError(f'invalid query: {table.__name__}.{fieldname} does not exist')


def decode_row(table, row):
    values = {}
    pk = ()
    for name, ty in table.__fields_info__.items():
        columns = len(list(columns_for(name, ty)))
        segment = row[:columns]
        if name in table.__primary_key__:
            pk += segment
        values[name] = from_stored(segment, ty)
        row = row[columns:]
    assert not row
    return table.get_object(pk, values)


T = TypeVar('T')


class Query(Generic[T]):
    """Represents one or more queries on a specified table.

    :samp:`Query({table}, filters)` is equivalent to :samp:`{table}.query(**filters)`"""
    table: Type[T]
    filters: Dict[str, Comparison]
    comparisons: str
    values: Dict[str, Any]

    def __init__(self, table: Type[T], filters: Dict[str, Any]) -> None:
        self.table = table
        self.filters = {
            key: ensure_comparison(value) for key, value
            in filters.items()}
        values = [
            (column, value.op, cooked)
            for key, value in self.filters.items()
            for column, cooked
            in encode_query_value(table, key, value.value).items()]
        self.values = {column: cooked for column, _, cooked in values}
        self.comparisons = ' and '.join(
            f'{column}{op}:{column}'
            for column, op, _ in values)

    def __len__(self) -> int:
        """Returns the number of rows matching this query.

        .. note:: This method accesses the connected database.

        :returns: number of matches
        :rtype: int
        """
        c, = execute(sql.count(self.table, self.comparisons), self.values).fetchone()
        return c

    def select_with_limit(self, limit: Optional[int] = None) -> Iterator[T]:
        """Create an iterator over the results of this query.

        .. note:: This method accesses the connected database.

        :param limit: The number of results to limit this query to.
        :type limit: int or None
        :returns: an iterator over the objects matching this query."""
        if limit is not None:
            values = {'_limit_': limit, **self.values}
        else:
            values = self.values
        for row in execute(sql.select(self.table, self.comparisons, limit is not None), values):
            yield decode_row(self.table, row)

    def __iter__(self) -> Iterator[T]:
        """Iterate over the results of this query.

        .. note:: This method accesses the connected database.

        Equivalent to :meth:`select_with_limit` without specifying *limit*."""
        return self.select_with_limit()

    def _only_first(self, *, of: int) -> T:
        try:
            i, = self.select_with_limit(of)
        except ValueError as e:
            raise WurmError(e.args[0].replace('values to unpack', 'rows returned')) from None
        return i

    def first(self) -> T:
        """Return the first result of this query.

        .. note:: This method accesses the connected database.

        :raises WurmError: if this query returns zero results"""
        return self._only_first(of=1)

    def one(self) -> T:
        """Return the only result of this query.

        .. note:: This method accesses the connected database.

        :raises WurmError: if this query returns zero results or more than one"""
        return self._only_first(of=2)

    def delete(self) -> None:
        """Delete the objects matching this query.

        .. warning:: Calling this on an empty query deletes all rows
           of the relevant table in the database

        .. note:: This method accesses the connected database.

        :returns: the number of rows deleted
        :rtype: int
        """
        return execute(
            sql.delete(self.table, self.comparisons),
            self.values).rowcount
