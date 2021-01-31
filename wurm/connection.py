from contextvars import ContextVar
import sqlite3

connection = ContextVar('connection')

class WurmError(Exception):
    """General error for a database operation failing.

    Its ``__cause__`` attribute refers to the relevant
    :class:`sqlite3.Error` when that exists."""

def execute(*args, conn=None):
    if conn is None:
        try:
            conn = connection.get()
        except LookupError:
            raise WurmError('setup_connection() not called in current'
                ' context!') from None
    try:
        with conn:
            return conn.execute(*args)
    except sqlite3.Error as e:
        raise WurmError from e

def setup_connection(conn):
    """Call this once in each OS thread with a
    :class:`sqlite3.Connection`, before accessing the database via wurm.

    This records the connection and ensures all tables are created."""
    token = connection.set(conn)
    execute('PRAGMA foreign_keys = ON', conn=conn)
    from .tables import BaseTable, create_tables
    create_tables(BaseTable, conn)
    return token

def close_connection(token):
    connection.reset(token)
