wurm
====

Wurm is a simple sqlite3-based ORM.

.. contents:: **Table of Contents**
    :backlinks: none

Usage
-----

.. code-block:: python

    # create a table:

    @dataclass
    class Point(wurm.Table):
        x: int
        y: int

    # types currently supported: int, str, bytes, bool, float, datetime.time,
    #     datetime.date, datetime.datetime, pathlib.Path

    # sqlite3 connections cannot be shared, so call setup_connection once per thread

    wurm.setup_connection(sqlite3.connect(":memory:"))

    # adding new instances to the database:

    point = Point(1, 0)
    print(point.rowid) # None
    point.insert()
    print(point.rowid) # 1

    # making changes:

    point.x = 2
    point.commit()

    # simple queries:

    point = Point[1] # get by rowid
    del Point[1] # delete by rowid
    point.delete() # delete from an object
    all_points = list(Point) # iterate over a table to get instances for all rows
    number_of_points = len(Point) # get the total number of rows in the table



Installation
------------

wurm is distributed on `PyPI <https://pypi.org>`_ as a universal
wheel and is available on Linux/macOS and Windows and supports
Python 3.7+.

.. code-block:: bash

    $ pip install wurm

Changelog
---------

0.0.2
=====

* Ensure tables are created, even in edge cases.
* Add support for ``date``, ``time``, ``datetime`` and ``Path``.
* Add ``wurm.Unique[T]``.


License
-------

wurm is distributed under the terms of the
`MIT License <https://choosealicense.com/licenses/mit>`_.
