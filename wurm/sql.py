from .typemaps import sql_type_for, columns_for

def create_fields(table):
    return ", ".join(sql_type_for(name, ty)
        for name, ty in table.__fields_info__.items())

def create_primary_key(table):
    field_info = table.__fields_info__
    return ", ".join(column for field in table.__primary_key__
            for column in columns_for(field, field_info[field]))

def create_indexes(table):
    table_name = table.__table_name__
    field_info = table.__fields_info__
    for field, unique in table.__indexes__:
        yield (f'create {unique and "unique" or ""} index if not exists'
            f' {table_name}_{field}_index on {table_name} '
            f'({", ".join(columns_for(field, field_info[field]))})')

def create(table):
    return (f'create table if not exists {table.__table_name__}'
        f'({create_fields(table)}, '
        f'PRIMARY KEY ({create_primary_key(table)}))')

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
        limit_clause = 'limit :_limit_'
    else:
        limit_clause = ''
    return f'select * from {table.__table_name__} {where_clause} {limit_clause}'

def insert(table):
    columns = [column for field, ty in table.__fields_info__.items()
        for column in columns_for(field, ty)]
    return f'insert into {table.__table_name__} ({", ".join(columns)}) values({", ".join(":" + column for column in columns)})'

def update(table):
    return f'''update {table.__table_name__} set {", ".join(f"{column}=:{column}" for field in table.__data_fields__
        for column in columns_for(field, table.__fields_info__[field])) } where {", ".join(f"{column}=:{column}" for field in table.__primary_key__
        for column in columns_for(field, table.__fields_info__[field])) }'''

def delete(table, where=None):
    if where:
        where_clause = f'where {where}'
    else:
        where_clause = ''
    return f'delete from {table.__table_name__} {where_clause}'
