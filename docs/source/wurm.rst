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

.. autoclass:: wurm.Table
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

.. data:: wurm.Unique

   Using :samp:`Unique[{T}]` as a type annotation in a table definition
   is equivalent to using :samp:`{T}`, except that a ``UNIQUE`` index
   is created for the field. Note that SQL considers ``None`` values to
   be different from other ``None`` values for this purpose.

   If you attempt to call :meth:`Table.insert` or :meth:`Table.commit`
   in a way that would violate such a constraint, the operation is
   rolled back, and a :class:`WurmError` is raised.

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

