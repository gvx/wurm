from .typemaps import sql_type_for

def create(table):
    return f'create table if not exists {table.__table_name__}({", ".join(make_field(name, ty) for name, ty in table.__fields_info__.items())}, PRIMARY KEY ({", ".join(table.__primary_key__)}))'

def make_field(name, ty):
    return f'{name} {sql_type_for(ty)}'

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
    return f'select * from {table.__table_name__} {where_clause} {limit_clause}'

def insert(table):
    return f'insert into {table.__table_name__} ({", ".join(name for name in table.__fields_info__)}) values({", ".join(":" + name for name in table.__fields_info__)})'

def update(table):
    return f'update {table.__table_name__} set {", ".join(f"{name}=:{name}" for name in table.__datafields__) } where {", ".join(f"{name}=:{name}" for name in table.__primary_key__) }'

def delete(table, where=None):
    if where:
        where_clause = f'where {where}'
    else:
        where_clause = ''
    return f'delete from {table.__table_name__} {where_clause}'
