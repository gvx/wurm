__version__ = '0.0.1'

from .typemaps import register_type
from .tables import WurmError, Table, setup_connection

__all__ = ['register_type', 'WurmError', 'Table', 'setup_connection']
