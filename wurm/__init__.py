__version__ = '0.0.2'

from .typemaps import register_type, Unique
from .tables import Table, setup_connection
from .connection import WurmError
from .queries import lt, gt, le, ge, ne, eq, Query

__all__ = ['register_type', 'Unique', 'WurmError', 'Table',
    'setup_connection', 'lt', 'gt', 'le', 'ge', 'ne', 'eq', 'Query']
