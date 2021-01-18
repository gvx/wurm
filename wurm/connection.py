from contextvars import ContextVar
import sqlite3

connection = ContextVar('connection')

class WurmError(Exception):
    """General error for a database operation failing.

    Its ``__cause__`` attribute refers to the relevant
    :class:`sqlite3.Error` when that exists."""

def execute(*args, conn=None):
    with open('.sql.log', 'a') as f:
        print(*args, file=f)
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
