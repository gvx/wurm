# PRELUDE
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent))

# SEGMENT 1
from dataclasses import dataclass
from wurm import Table, Unique

@dataclass
class NamedPoint(Table):
    x: int
    y: int
    name: Unique[str]

# SEGMENT 2
from wurm import setup_connection
import sqlite3

setup_connection(sqlite3.connect(':memory:'))

# SEGMENT 3
basecamp = NamedPoint(x=1, y=2, name='Basecamp')

print(basecamp)

basecamp.insert()

print(basecamp.rowid)

NamedPoint(x=10, y=-7, name='Goal').insert()

print(list(NamedPoint))
print(NamedPoint.query(x=10).one())

# SEGMENT 4
"""
NamedPoint(x=1, y=2, name='Basecamp')
1
[NamedPoint(x=1, y=2, name='Basecamp'), NamedPoint(x=10, y=-7, name='Goal')]
NamedPoint(x=10, y=-7, name='Goal')
"""
