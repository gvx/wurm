from dataclasses import dataclass, field, astuple, fields
from typing import ClassVar, Tuple, Dict, get_type_hints

from .typemaps import to_stored, Primary, is_primary
from .queries import Query
from .connection import execute, connection
from . import sql

def table_fields(table):
    return {key: value for key, value in get_type_hints(table, include_extras=True).items()
        if not key.startswith('_')}

def primary_key_fields(fields):
    return tuple(key for key, value in fields.items()
        if is_primary(value))

def data_fields(fields):
    return tuple(key for key, value in fields.items()
        if not is_primary(value))

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None, abstract=False):
        t = super().__new__(cls, clsname, bases, classdict)
        t.__abstract__ = abstract
        if abstract:
            t.__table_name__ = '<abstract table>'
        else:
            t.__table_name__ = name or clsname
            t.__fields_info__ = table_fields(t)
            t.__primary_key__ = primary_key_fields(t.__fields_info__)
            t.__datafields__ = data_fields(t.__fields_info__)
        return t
    def __iter__(self):
        """Iterate over all the objects in the table.

        .. note:: This method accesses the connected database.

        """
        return iter(self.query())
    def __bool__(self):
        return True
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
        the relevant fields, or the same wrapped in one of
        :func:`~wurm.lt`, :func:`~wurm.gt`, :func:`~wurm.le`,
        :func:`~wurm.ge`, :func:`~wurm.eq` or :func:`~wurm.ne`.
        When unwrapped, the behavior matches that of values wrapped in
        :func:`~wurm.eq`.

        Merely creating a query does not access the database.

        :returns: A query for this table.
        :rtype: Query"""
        return Query(self, kwargs)

@dataclass
class WithoutRowid(metaclass=TableMeta, abstract=True):
    """Baseclass for your own tables. Tables must be dataclasses.

    Use the keyword argument *name* in the class definition to set the
    table name::

        @dataclass
        class MyTable(Table, name='mytable'):
            ...

    If not given, wurm uses the class name to automatically derive a
    suitable table name."""
    __fields_info__: ClassVar[Dict[str, type]]
    __datafields__: ClassVar[Tuple[str, ...]]
    __primary_key__: ClassVar[Tuple[str, ...]]
    __abstract__: ClassVar[bool]
    __table_name__: ClassVar[str]
    def __new__(cls, *args, **kwargs):
        if cls.__abstract__:
            raise TypeError('cannot instantiate abstract table')
        return super().__new__(cls)
    def insert(self):
        """Insert a new object into the database.

        .. note:: This method accesses the connected database.

        """
        cursor = execute(sql.insert(type(self)), self._encode_row())
        self.rowid = cursor.lastrowid
    def commit(self):
        """Commits any changes to the object to the database.

        .. note:: This method accesses the connected database.

        """
        execute(sql.update(type(self)), self._encode_row())
    def delete(self):
        """Deletes this object from the database.

        .. note:: This method accesses the connected database.

        :raises ValueError: if called twice on the same instance, or
            called on a fresh instance that has not been inserted yèt.

        """
        Query(type(self), self._primary_key()).delete()
    def _primary_key(self):
        return {key: getattr(self, key) for key in self.__primary_key__}
    def _encode_row(self):
        return {key: to_stored(ty, getattr(self, key)) for key, ty in self.__fields_info__.items()}

@dataclass
class Table(WithoutRowid, abstract=True):
    # technically rowid is Optional[int], but that's not implemented yet
    rowid: Primary[int] = field(init=False, default=None, compare=False, repr=False)
    def delete(self):
        """Deletes this object from the database.

        .. note:: This method accesses the connected database.

        :raises ValueError: if called twice on the same instance, or
            called on a fresh instance that has not been inserted yèt.

        """
        if self.rowid is None:
            raise ValueError('Cannot delete instance not in database')
        super().delete()
        self.rowid = None

def create_tables(tbl, conn):
    for table in tbl.__subclasses__():
        if not table.__abstract__:
            execute(sql.create(table), conn=conn)
        create_tables(table, conn)

def setup_connection(conn):
    """Call this once in each OS thread with a
    :class:`sqlite3.Connection`, before accessing the database via wurm.

    This records the connection and ensures all tables are created."""
    connection.set(conn)
    create_tables(WithoutRowid, conn)
