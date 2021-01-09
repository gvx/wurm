from contextvars import ContextVar
from dataclasses import dataclass, field, astuple, fields
from typing import ClassVar

from . import sql
from .typemaps import to_stored, from_stored

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None):
        t = super().__new__(cls, clsname, bases, classdict)
        t.__table_name__ = clsname if name is None else name
        return t
    def __getitem__(self, pk):
        ensure_created(self)
        return decode_row(self, connection.get().execute(sql.select_rowid(self), (pk,)).fetchone())
    def __delitem__(self, pk):
        ensure_created(self)
        connection.get().execute(sql.delete(self), (pk,))
    def __iter__(self):
        ensure_created(self)
        for row in connection.get().execute(sql.select(self)):
            yield decode_row(self, row)
    def __len__(self):
        ensure_created(self)
        c, = connection.get().execute(sql.count(self)).fetchone()
        return c
    # def __reversed__(self):
    #     pass

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
    __table_created__: ClassVar[bool] = False
    # technically rowid is Optional[int], but that's not implemented yet
    rowid: int = field(init=False, default=None, compare=False, repr=False)
    def insert(self):
        ensure_created(type(self))
        cursor = connection.get().execute(sql.insert(type(self), includes_rowid=self.rowid is not None),
            encode_row(self))
        self.rowid = cursor.lastrowid
    def commit(self):
        ensure_created(type(self))
        assert self.rowid is not None
        connection.get().execute(sql.update(type(self)), encode_row(self, exclude_rowid=True) + (self.rowid,))
    def delete(self):
        del type(self)[self.rowid]
        self.rowid = None

connection = ContextVar('connection')

def ensure_created(table):
    if not table.__table_created__:
        try:
            conn = connection.get()
        except LookupError:
            return
        conn.execute(sql.create(table))
        table.__table_created__ = True

def setup_connection(con):
    connection.set(con)
    for table in Table.__subclasses__():
        con.execute(sql.create(table))
        table.__table_created__ = True
