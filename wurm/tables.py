from dataclasses import dataclass, field
import sys
from typing import ClassVar, Tuple, Dict, get_type_hints
from types import MappingProxyType
from weakref import WeakValueDictionary

from .typemaps import (to_stored, Primary, is_primary, unwrap_type,
    is_unique)
from .queries import Query
from .connection import execute
from . import sql

def table_fields(table):
    return {key: value for key, value
        in get_type_hints(table, include_extras=True).items()
        if not key.startswith('_')}

def primary_key_fields(fields):
    return tuple(key for key, value in fields.items()
        if is_primary(value))

def data_fields(fields):
    return tuple(key for key, value in fields.items()
        if not is_primary(value))

def indexes(fields):
    return tuple((key, True) for key, value in fields.items()
        if is_unique(value))

def primary_key_columns(item):
    for fieldname, ty in item.__fields_info__.items():
        if fieldname in item.__primary_key__:
            yield from to_stored(fieldname, ty,
                getattr(item, fieldname)).values()

class relation:
    """Describe a relationship between two tables.

    :param str target: The name of either the target table, or the
        specific field being referenced.
    :param str lazy: How the relationship is loaded. Possible options:
        ``'select'`` (the default): the relationship is loaded lazily,
        as a list of target objects; ``'query'``: the relationship is
        loaded lazily, as a query on the target model; ``'strict'``: the
        relationship is loaded as a list when objects of the current
        model are loaded."""
    def __init__(self, target: str, lazy: str = 'select'):
        self.target = target
        assert lazy in {'select', 'query', 'strict'}
        self.lazy = lazy
    def _find_target(self, owner):
        search_path = self.target.split('.')
        ns = self.namespace
        for name in search_path[:-1]:
            ns = getattr(ns, name)
        if isinstance(ns, TableMeta):
            target_attr = search_path[-1]
        else:
            ns = getattr(ns, search_path[-1])
            target_attr = None
        if not isinstance(ns, TableMeta):
            raise TypeError(f'invalid target {self.target} for '
                f'{owner.__name__}.{self.name}')
        target_table = ns
        if target_attr is None:
            possible_attrs = [fieldname
                for fieldname, ty
                in target_table.__fields_info__.items()
                if ty is owner]
            if not possible_attrs:
                raise TypeError(f'Model {target_table.__name__} does '
                    f'not have a {owner.__name__} field, so the '
                    f'relation {owner.__name__}.{self.name} is invalid.')
            if len(possible_attrs) > 1:
                raise TypeError(f'Model {target_table.__name__} has '
                    f'multiple {owner.__name__} fields: '
                    f'{", ".join(possible_attrs[:-1])} and '
                    f'{possible_attrs[-1]}. Specify the right field for '
                    f'the relation {owner.__name__}.{self.name}.')
            target_attr, = possible_attrs
        elif target_table.__fields_info__[target_attr] is not owner:
            raise TypeError(f'{self.target} is not {owner.__name__}, '
                f'so the relation {owner.__name__}.{self.name} is invalid.')
        self.target_table = target_table
        self.target_attr = target_attr
    def __set_name__(self, owner, name):
        self.name = name
        self.namespace = sys.modules[owner.__module__]
    def __get__(self, instance, owner=None):
        if instance is None:
            return self # FIXME: relation on class?
        if not hasattr(self, 'target_table'):
            self._find_target(owner)
        q = Query(self.target_table, {self.target_attr: instance})
        if self.lazy == 'query':
            return q
        else:
            return list(q)

def strict_relations(classdict):
    for key, value in classdict.items():
        if isinstance(value, relation) and value.lazy == 'strict':
            yield key

class TableMeta(type):
    def __new__(cls, clsname, bases, classdict, name=None, abstract=False):
        if not all(getattr(base, '__abstract__', True) for base in bases):
            raise TypeError('cannot subclass non-abstract Table')
        t = super().__new__(cls, clsname, bases, classdict)
        t.__abstract__ = abstract
        if abstract:
            t.__table_name__ = '<abstract table>'
        else:
            t.__table_name__ = name or clsname
            fields = table_fields(t)
            t.__fields_info__ = MappingProxyType({key: unwrap_type(ty) for key, ty in fields.items()})
            t.__primary_key__ = primary_key_fields(fields)
            t.__data_fields__ = data_fields(fields)
            t.__indexes__ = indexes(fields)
            t.__strict_relations__ = tuple(strict_relations(classdict))
            t.__id_map__ = WeakValueDictionary()
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
    def get_object(self, pk, values):
        if pk not in self.__id_map__:
            if 'rowid' in values:
                rowid = values.pop('rowid')
            else:
                rowid = ...
            self.__id_map__[pk] = item = self(**values)
            if rowid is not ...:
                item.rowid = rowid
            for rel in self.__strict_relations__:
                item.__dict__[rel] = getattr(item, rel)
        return self.__id_map__[pk]
    def add_object(self, item):
        self.__id_map__[tuple(primary_key_columns(item))] = item
    def del_object(self, item):
        del self.__id_map__[tuple(primary_key_columns(item))]

@dataclass
class BaseTable(metaclass=TableMeta, abstract=True):
    """Baseclass for your own tables. Tables must be dataclasses.

    Do not use directly, subclass :class:`wurm.Table` or
    :class:`wurm.WithoutRowid` instead.

    Use the keyword argument *name* in the class definition to set the
    table name::

        @dataclass
        class MyTable(Table, name='mytable'):
            ...

    If not given, wurm uses the class name to automatically derive a
    suitable table name.

    Use the keyword argument *abstract* in the class definition to
    add fields or methods that you want to share between several
    tables::

        @dataclass
        class HasOwner(Table, abstract=True):
            owner: str
            def display_owner(self):
                return self.owner.capitalize()

    The above will not create a new table, but subclasses of
    ``HasOwner`` will have a field called ``owner`` and a method
    called ``display_owner``.
    """
    __id_map__: ClassVar[dict]
    __fields_info__: ClassVar[Dict[str, type]]
    __data_fields__: ClassVar[Tuple[str, ...]]
    __primary_key__: ClassVar[Tuple[str, ...]]
    __indexes__: ClassVar[Tuple[Tuple[str, bool], ...]]
    __strict_relations__: ClassVar[Tuple[str, ...]]
    __abstract__: ClassVar[bool]
    __table_name__: ClassVar[str]
    def __new__(cls, *args, **kwargs):
        if cls.__abstract__:
            raise TypeError('cannot instantiate abstract table')
        return super().__new__(cls)
    def __post_init__(self):
        for rel in self.__strict_relations__:
            self.__dict__[rel] = []
    def insert(self):
        """Insert a new object into the database.

        .. note:: This method accesses the connected database.

        """
        cursor = execute(sql.insert(type(self)), self._encode_row())
        if 'rowid' in self.__primary_key__:
            self.rowid = cursor.lastrowid
        type(self).add_object(self)
    def commit(self):
        """Commits any changes to the object to the database.

        .. note:: This method accesses the connected database.

        """
        execute(sql.update(type(self)), self._encode_row())
    def delete(self):
        """Deletes this object from the database.

        .. note:: This method accesses the connected database.

        :raises ValueError: if called twice on the same instance, or
            called on a fresh instance that has not been inserted yet.

        """
        Query(type(self), self._primary_key()).delete()
        type(self).del_object(self)
    def _primary_key(self):
        return {key: getattr(self, key) for key in self.__primary_key__}
    def _encode_row(self):
        return {column: cooked
            for key, ty in self.__fields_info__.items()
            for column, cooked
            in to_stored(key, ty, getattr(self, key)).items()}

@dataclass
class WithoutRowid(BaseTable, abstract=True):
    pass

@dataclass
class Table(BaseTable, abstract=True):
    # technically rowid is Optional[int], but that's not implemented yet
    rowid: Primary[int] = field(init=False, default=None, compare=False, repr=False)
    def delete(self):
        """Deletes this object from the database.

        .. note:: This method accesses the connected database.

        :raises ValueError: if called twice on the same instance, or
            called on a fresh instance that has not been inserted yet.

        """
        if self.rowid is None:
            raise ValueError('Cannot delete instance not in database')
        super().delete()
        self.rowid = None

def create_tables(tbl, conn):
    for table in tbl.__subclasses__():
        if not table.__abstract__:
            execute(sql.create(table), conn=conn)
            for create_index_query in sql.create_indexes(table):
                execute(create_index_query, conn=conn)
        create_tables(table, conn)

