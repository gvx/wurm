from dataclasses import dataclass, field, astuple, fields
from typing import ClassVar

from .typemaps import to_stored
from .queries import Query, decode_row
from .connection import execute, ensure_created, connection
from . import sql

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None):
        t = super().__new__(cls, clsname, bases, classdict)
        t.__table_name__ = clsname if name is None else name
        return t
    def __iter__(self):
        return iter(self.query())
    def __len__(self):
        return len(self.query())
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
        if self.rowid is None:
            raise ValueError('Cannot delete instance not in database')
        type(self).query(rowid=self.rowid).delete()
        self.rowid = None

def setup_connection(conn):
    connection.set(conn)
    for table in Table.__subclasses__():
        ensure_created(table, conn=conn)
