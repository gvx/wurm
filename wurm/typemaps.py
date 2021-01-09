from typing import NamedTuple

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
    return TYPE_MAPPING[python_type].encode(value)

def from_stored(python_type, value):
    if value is None:
        return None
    return TYPE_MAPPING[python_type].decode(value)

def sql_type_for(python_type):
    return TYPE_MAPPING[python_type].sql_type

register_type(str, 'TEXT')
register_type(bytes, '')
register_type(int, 'INT')
register_type(float, 'REAL')
register_type(bool, 'INT', int, bool)
