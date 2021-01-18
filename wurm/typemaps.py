from datetime import datetime, date, time
from pathlib import Path
from typing import NamedTuple, TypeVar

try:
    from typing import Annotated, get_args, get_origin
except ImportError:  # noqa
    from typing_extensions import Annotated, get_args, get_origin

T = TypeVar('T')
_UniqueMarker = {'wurm-unique': True}
_PrimaryMarker = {'wurm-primary': True}

Unique = Annotated[T, _UniqueMarker]
Primary = Annotated[T, _PrimaryMarker]

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
            ...

        register_type(Foo, str, encode=repr, decode=Foo.from_string)

    :param type python_type: The type to register
    :param type sql_type: The stored type, one of :class:`int`,
        :class:`str`, :class:`float`, :class:`bytes`
    :param encode: The function to prepare to store a value in the
        database
    :type encode: Callable[[python_type], sql_type]
    :param decode: The function to interpret the stored value
    :type decode: Callable[[sql_type], python_type]"""
    assert python_type not in TYPE_MAPPING
    TYPE_MAPPING[python_type] = StoredValueTypeMap(SQL_EQUIVALENTS[sql_type], encode, decode)

def to_stored(python_type, value):
    if value is None:
        return None
    if get_origin(python_type) is Annotated:
        python_type = get_args(python_type)[0]
    return TYPE_MAPPING[python_type].encode(value)

def from_stored(python_type, value):
    if value is None:
        return None
    if get_origin(python_type) is Annotated:
        python_type = get_args(python_type)[0]
    return TYPE_MAPPING[python_type].decode(value)

def sql_type_for(python_type):
    postfix = ''
    if get_origin(python_type) is Annotated:
        python_type, *rest = get_args(python_type)
        if any(_PrimaryMarker is arg for arg in rest):
            postfix = ' PRIMARY KEY'
        elif any(_UniqueMarker is arg for arg in rest):
            postfix = ' UNIQUE'
    return TYPE_MAPPING[python_type].sql_type + postfix

register_type(str, str, encode=passthrough, decode=passthrough)
register_type(bytes, bytes, encode=passthrough, decode=passthrough)
register_type(int, int, encode=passthrough, decode=passthrough)
register_type(float, float, encode=passthrough, decode=passthrough)
register_type(bool, int, encode=int, decode=bool)
register_type(date, str, encode=date.isoformat, decode=date.fromisoformat)
register_type(time, str, encode=time.isoformat, decode=time.fromisoformat)
register_type(datetime, str, encode=datetime.isoformat, decode=datetime.fromisoformat)
register_type(Path, str, encode=str, decode=Path)
