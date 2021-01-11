from dataclasses import dataclass, field, astuple, fields
from typing import ClassVar

from .typemaps import to_stored
from .queries import Query
from .connection import execute, ensure_created, connection
from . import sql

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None):
        t = super().__new__(cls, clsname, bases, classdict)
        t.__table_name__ = clsname if name is None else name
        return t
    def __iter__(self):
        """Iterate over all the objects in the table.

        .. note:: This method accesses the connected database.

        """
        return iter(self.query())
    def __len__(self):
        """The total number of rows in this table.

        .. note:: This method accesses the connected database.

        """
        return len(self.query())
    def query(self, **kwargs):
        """Create a query object.

        The names of keywords passed should be
        *rowid* or any of the fields defined on the table.

        The values can either be Python values matching the types of
        the relevant fields, or the same wrapped in one of :py:`lt`,
        :py:`gt`, :py:`le`, :py:`ge`, :py:`eq` or :py:`ne`.
        When unwrapped, the behavior matches that of values wrapped in
        :py:`eq`.

        Merely creating a query does not access the database.

        :returns: A query for this table.
        :rtype: Query"""
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
    """Baseclass for your own tables. Tables must be dataclasses.

    Use the keyword argument *name* to set the table name::

        @dataclass
        class MyTable(Table, name='mytable'):
            ...

    If not given, wurm uses the class name to automatically derive a
    suitable table name."""
    __table_name__: ClassVar[str]
    # technically rowid is Optional[int], but that's not implemented yet
    rowid: int = field(init=False, default=None, compare=False, repr=False)
    def insert(self):
        """Insert a new object into the database.

        .. note:: This method accesses the connected database.

        """
        ensure_created(type(self))
        cursor = execute(sql.insert(type(self), includes_rowid=self.rowid is not None),
            encode_row(self))
        self.rowid = cursor.lastrowid
    def commit(self):
        """Commits any changes to the object to the database.

        .. note:: This method accesses the connected database.

        """
        ensure_created(type(self))
        assert self.rowid is not None
        execute(sql.update(type(self)), encode_row(self, exclude_rowid=True) + (self.rowid,))
    def delete(self):
        """Deletes this object from the database.

        .. note:: This method accesses the connected database.

        """
        if self.rowid is None:
            raise ValueError('Cannot delete instance not in database')
        type(self).query(rowid=self.rowid).delete()
        self.rowid = None

def setup_connection(conn):
    """Call this once in each Thread with a sqlite3.Connection, before
    accessing the database via wurm.

    This records the connection and ensures all tables are created."""
    connection.set(conn)
    for table in Table.__subclasses__():
        ensure_created(table, conn=conn)
