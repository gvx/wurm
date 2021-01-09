from datetime import datetime, date, time
from pathlib import Path
from typing import NamedTuple, TypeVar

try:
    from typing import Annotated, get_args, get_origin
except ImportError:  # noqa
    from typing_extensions import Annotated, get_args, get_origin

T = TypeVar('T')
_UniqueMarker = {'wurm-unique': True}

Unique = Annotated[T, _UniqueMarker]

class StoredValueTypeMap(NamedTuple):
    sql_type: str
    encode: callable
    decode: callable

TYPE_MAPPING = {}

def passthrough(x):
    return x

def register_type(python_type, sql_type, encode=passthrough, decode=passthrough):
    assert python_type not in TYPE_MAPPING
    TYPE_MAPPING[python_type] = StoredValueTypeMap(sql_type, encode, decode)

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
        if any(_UniqueMarker is arg for arg in rest):
            postfix = ' UNIQUE'
    return TYPE_MAPPING[python_type].sql_type + postfix

register_type(str, 'TEXT')
register_type(bytes, '')
register_type(int, 'INT')
register_type(float, 'REAL')
register_type(bool, 'INT', int, bool)
register_type(date, 'TEXT', date.isoformat, date.fromisoformat)
register_type(time, 'TEXT', time.isoformat, time.fromisoformat)
register_type(datetime, 'TEXT', datetime.isoformat, datetime.fromisoformat)
register_type(Path, 'TEXT', str, Path)
