from datetime import datetime, date, time
from pathlib import Path
from typing import NamedTuple, TypeVar
from types import MappingProxyType

try:
    from typing import Annotated, get_args, get_origin
except ImportError:  # noqa
    from typing_extensions import Annotated, get_args, get_origin

T = TypeVar('T')
_UniqueMarker = {'wurm-unique': True}
_PrimaryMarker = {'wurm-primary': True}

Unique = Annotated[T, _UniqueMarker]
Primary = Annotated[T, _PrimaryMarker]

def is_unique(ty: type) -> bool:
    if get_origin(ty) is Annotated:
        _, *args = get_args(ty)
        return any(_UniqueMarker is arg for arg in args)
    return False

def is_primary(ty: type) -> bool:
    if get_origin(ty) is Annotated:
        _, *args = get_args(ty)
        return any(_PrimaryMarker is arg for arg in args)
    return False

class StoredValueTypeMap(NamedTuple):
    sql_type: str
    encode: callable
    decode: callable

TYPE_MAPPING = {}

SQL_EQUIVALENTS = {int: 'INTEGER', str: 'TEXT', float: 'REAL', bytes: ''}

def passthrough(x):
    return x

def register_type(python_type, sql_type, *, encode, decode):
    """Registers a type for use in model fields.

    For example::

        class Foo:
            def __repr__(self):
                ...
            @classmethod
            def from_string(cls, string):
                ...
            ...

        register_type(Foo, str, encode=repr, decode=Foo.from_string)

    :param type python_type: The type to register
    :param sql_type: The stored type, one of :class:`int`,
        :class:`str`, :class:`float`, :class:`bytes`, a dict
        from :class:`str` to the above primitive types, or a tuple of
        the above primitive types. The latter two options is for types
        that should be mapped to multiple columns.
    :param encode: The function to prepare to store a value in the
        database. Should return a tuple if the type is mapped to
        multiple columns.
    :type encode: python_type -> sql_type
    :param decode: The function to interpret the stored value. Should
        take multiple arguments in the case types mapped to multiple
        columns.
    :type decode: sql_type -> python_type"""
    assert python_type not in TYPE_MAPPING
    if isinstance(sql_type, dict):
        sql_equiv = MappingProxyType({key: SQL_EQUIVALENTS[ty] for key, ty in sql_type.items()})
    elif isinstance(sql_type, tuple):
        sql_equiv = MappingProxyType({str(i): SQL_EQUIVALENTS[ty] for i, ty in enumerate(sql_type)})
    else:
        sql_equiv = SQL_EQUIVALENTS[sql_type]
    TYPE_MAPPING[python_type] = StoredValueTypeMap(sql_equiv, encode, decode)

def register_dataclass(dclass):
    '''Registers a dataclass for use in model fields.

    This is a convenience function that can optionally be used as a
    decorator. Given::

        @dataclasses.dataclass
        class Color:
            r: float
            g: float
            b: float

    then the following::

        register_dataclass(Color)

    is equivalent to::

        register_type(Color, dict(r=float, g=float, b=float),
            encode=dataclasses.astuple, decode=Color)

    In either case, the model::

        class MyTable(Table):
            color: Color

    will have the fields ``color_r``, ``color_g`` and ``color_b``, which
    will transparently be converted to and from ``Color`` objects.

    :param dclass: The dataclass to register
    :returns: The registered dataclass
    '''
    from dataclasses import astuple, is_dataclass, fields
    assert is_dataclass(dclass)
    register_type(dclass,
        {field.name: field.type for field in fields(dclass)},
        encode=astuple, decode=dclass)
    return dclass

def columns_for(field_name, python_type):
    from .tables import BaseTable
    if issubclass(python_type, BaseTable):
        return (f'{field_name}_{pk}'
                for pk in python_type.__primary_key__)
    typemap = TYPE_MAPPING[python_type]
    if isinstance(typemap.sql_type, MappingProxyType):
        return (f'{field_name}_{key}' for key in typemap.sql_type)
    return field_name,

def to_stored(field_name, python_type, value):
    if value is None:
        return dict.fromkeys(columns_for(field_name, python_type))

    from .tables import BaseTable
    if issubclass(python_type, BaseTable):
        assert isinstance(value, python_type)
        return {column_name: val
            for pk in python_type.__primary_key__
            for column_name, val
            in to_stored(f'{field_name}_{pk}',
                python_type.__fields_info__[pk], getattr(value, pk)).items()}

    typemap = TYPE_MAPPING[python_type]
    if isinstance(typemap.sql_type, MappingProxyType):
        return {f'{field_name}_{key}': val for key, val in zip(typemap.sql_type, typemap.encode(value))}
    return {field_name: typemap.encode(value)}

def unwrap_type(ty):
    if get_origin(ty) is Annotated:
        return get_args(ty)[0]
    return ty

def from_stored(stored_tuple, python_type):
    if all(v is None for v in stored_tuple):
        return None
    from .tables import BaseTable
    if issubclass(python_type, BaseTable):
        # FIXME: this is problematic for recursive foreign keys
        # not to mention the n + 1 problem
        return python_type.get_object(stored_tuple, None)
    return TYPE_MAPPING[python_type].decode(*stored_tuple)

def sql_type_for(fieldname, python_type):
    from .tables import BaseTable
    if issubclass(python_type, BaseTable):
        pk = python_type.__primary_key__
        info = python_type.__fields_info__
        return ', '.join(sql_type_for(f'{fieldname}_{key}', info[key])
            for key in pk)

    mapped_type = TYPE_MAPPING[python_type].sql_type
    if isinstance(mapped_type, MappingProxyType):
        return ', '.join(f'{fieldname}_{key} {ty}' for key, ty
            in mapped_type.items())
    return f'{fieldname} {mapped_type}'

register_type(str, str, encode=passthrough, decode=passthrough)
register_type(bytes, bytes, encode=passthrough, decode=passthrough)
register_type(int, int, encode=passthrough, decode=passthrough)
register_type(float, float, encode=passthrough, decode=passthrough)
register_type(bool, int, encode=int, decode=bool)
register_type(date, str, encode=date.isoformat, decode=date.fromisoformat)
register_type(time, str, encode=time.isoformat, decode=time.fromisoformat)
register_type(datetime, str, encode=datetime.isoformat, decode=datetime.fromisoformat)
register_type(Path, str, encode=str, decode=Path)
