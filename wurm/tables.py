from dataclasses import dataclass, field, astuple, fields
from typing import ClassVar

from .typemaps import to_stored, from_stored
from .queries import Query
from .connection import execute, ensure_created, connection
from . import sql

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None):
        t = super().__new__(cls, clsname, bases, classdict)
        t.__table_name__ = clsname if name is None else name
        return t
    def __getitem__(self, pk):
        ensure_created(self)
        return decode_row(self, execute(sql.select_rowid(self), (pk,)).fetchone())
    def __delitem__(self, pk):
        ensure_created(self)
        execute(sql.delete(self), (pk,))
    def __iter__(self):
        ensure_created(self)
        for row in execute(sql.select(self)):
            yield decode_row(self, row)
    def __len__(self):
        ensure_created(self)
        c, = execute(sql.count(self)).fetchone()
        return c
    def query(self, **kwargs):
        return Query(self, kwargs)

def encode_row(item, *, exclude_rowid=False):
    row = tuple(
        to_stored(field.type, py_value)
        for py_value, field
        in zip(astuple(item), fields(item)))
    if item.rowid is None or exclude_rowid:
        return row[1:]
    return row

def decode_row(table, row):
    rowid, *py_values = (
        from_stored(field.type, stored_value)
        for stored_value, field
        in zip(row, fields(table))
        )
    item = table(*py_values)
    item.rowid = rowid
    return item

@dataclass
class Table(metaclass=TableMeta, name=NotImplemented):
    __table_name__: ClassVar[str]
    # technically rowid is Optional[int], but that's not implemented yet
    rowid: int = field(init=False, default=None, compare=False, repr=False)
    def insert(self):
        ensure_created(type(self))
        cursor = execute(sql.insert(type(self), includes_rowid=self.rowid is not None),
            encode_row(self))
        self.rowid = cursor.lastrowid
    def commit(self):
        ensure_created(type(self))
        assert self.rowid is not None
        execute(sql.update(type(self)), encode_row(self, exclude_rowid=True) + (self.rowid,))
    def delete(self):
        del type(self)[self.rowid]
        self.rowid = None

def setup_connection(conn):
    connection.set(conn)
    for table in Table.__subclasses__():
        ensure_created(table, conn=conn)
