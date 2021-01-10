from dataclasses import fields

from .typemaps import sql_type_for

def create(table):
    return f'create table if not exists {table.__table_name__}({", ".join(make_field(f) for f in fields(table)[1:])})'

def make_field(f):
    return f'{f.name} {sql_type_for(f.type)}'

def count(table, where=None):
    if not where:
        where_clause = ''
    else:
        where_clause = f'where {where}'
    return f'select count(*) from {table.__table_name__} {where_clause}'

def select(table, where=None, limit=False):
    if where:
        where_clause = f'where {where}'
    else:
        where_clause = ''
    if limit:
        limit_clause = 'limit ?'
    else:
        limit_clause = ''
    return f'select rowid, * from {table.__table_name__} {where_clause} {limit_clause}'

def select_rowid(table):
    return f'select rowid, * from {table.__table_name__} where rowid=?'

def insert(table, *, includes_rowid=False):
    tfields = fields(table)
    if not includes_rowid:
        tfields = tfields[1:]
    return f'insert into {table.__table_name__} ({", ".join(f.name for f in tfields)}) values({", ".join("?" * len(tfields))})'

def update(table):
    return f'update {table.__table_name__} set {", ".join(f"{field.name}=?" for field in fields(table)[1:]) } where rowid=?'

def delete(table, where=None):
    if not where:
        where = 'rowid=?'
    return f'delete from {table.__table_name__} where {where}'
