=============
API reference
=============

------------------------
Connecting to a database
------------------------

.. autofunction:: wurm.setup_connection

---------------
Defining tables
---------------

.. autoclass:: wurm.tables.BaseTable
    :members:

    .. method:: query(**kwargs)
       :classmethod:

       Create a query object.

       The names of keywords passed should be
       *rowid* or any of the fields defined on the table.

       The values can either be Python values matching the types of
       the relevant fields, or the same wrapped in one of
       :func:`~wurm.lt`, :func:`~wurm.gt`, :func:`~wurm.le`,
       :func:`~wurm.ge`, :func:`~wurm.eq` or :func:`~wurm.ne`.
       When unwrapped, the behavior matches that of values wrapped in
       :func:`~wurm.eq`.

       Merely creating a query does not access the database.

       :returns: A query for this table.
       :rtype: Query

    .. method:: __len__
       :classmethod:

       The total number of rows in this table. A shortcut for :samp:`len({table}.query())`.

       .. note:: This method accesses the connected database.

    .. method:: __iter__
       :classmethod:

       Iterate over all the objects in the table. A shortcut for :samp:`iter({table}.query())`

       .. note:: This method accesses the connected database.

.. class:: wurm.Table

    Baseclass for regular rowid tables. See
    :class:`~wurm.tables.BaseTable` for methods available on
    subclasses.

.. class:: wurm.WithoutRowid

    Baseclass for ``WITHOUT ROWID`` tables. You need to add an explicit
    primary key using :data:`~wurm.Primary` for these kinds of tables.
    See :class:`~wurm.tables.BaseTable` for methods available on
    subclasses.

Annotations
***********

Tables are defined using :pep:`526` type annotations, where the
type for each column has to be one of the following:

* One of the basic supported types (currently :class:`str`,
  :class:`bytes`, :class:`int`, :class:`float`, :class:`bool`,
  :class:`datetime.date`, :class:`datetime.time`,
  :class:`datetime.datetime` and :class:`pathlib.Path`).
* A type registered with :func:`wurm.register_type`.
* A previously defined :class:`wurm.Table` or
  :class:`wurm.WithoutRowid` subclass.
* :samp:`wurm.Primary[{T}]`, :samp:`wurm.Index[{T}]` or
  :samp:`wurm.Unique[{T}]`, where
  :samp:`{T}` is one of the types mentioned above.


.. data:: wurm.Primary

   Using :samp:`Primary[{T}]` as a type annotation in a table definition
   is equivalent to using :samp:`{T}`, except that the column will be
   part of the primary key. If multiple fields on a single table
   definition are annotated in this way, their columns form a composite
   primary key together.

   If you attempt change the database in a way that would cause two
   rows to share a primary key, the operation is rolled back, and a
   :class:`~wurm.WurmError` is raised.

.. data:: wurm.Index

   Using :samp:`Index[{T}]` as a type annotation in a table definition
   is equivalent to using :samp:`{T}`, except that a (non-``UNIQUE``) index
   is created for the field.

.. data:: wurm.Unique

   Using :samp:`Unique[{T}]` as a type annotation in a table definition
   is equivalent to using :samp:`{T}`, except that a ``UNIQUE`` index
   is created for the field. Note that SQL considers ``None`` values to
   be different from other ``None`` values for this purpose.

   If you attempt to call :meth:`~wurm.tables.BaseTable.insert` or
   :meth:`~wurm.tables.BaseTable.commit` in a way that would violate
   such a constraint, the operation is rolled back, and a
   :class:`~wurm.WurmError` is raised.

.. autofunction:: wurm.register_type

.. autofunction:: wurm.register_dataclass

-------------
Queries
-------------

Query objects
*************

Most advanced queries will be done through :class:`Query` objects, that
can be created either explicitly through their constructor, or by
calling :meth:`Table.query`.

.. autoclass:: wurm.Query
   :members:
   :special-members: __len__, __iter__

Comparators
***********

.. function:: wurm.lt(value)
              wurm.le(value)
              wurm.eq(value)
              wurm.ne(value)
              wurm.ge(value)
              wurm.gt(value)

   Used to wrap values in queries. These functions correspond to the
   special names for the Python comparison operators.

   The expression ::

       MyTable.query(a=le(1), b=gt(2), c=3, d=ne(4))

   is roughly equivalent to

   .. code-block:: sql

      SELECT * FROM MyTable WHERE a <= 1 AND b > 2 AND c = 3 AND d != 4

   Replacing ``c=3`` with ``c=eq(3)`` is optional.

Exceptions
**********

.. autoexception:: wurm.WurmError

