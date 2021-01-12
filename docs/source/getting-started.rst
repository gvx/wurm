===============
Getting started
===============

Installation
------------

wurm is distributed on `PyPI <https://pypi.org>`_ as a universal
wheel and is available on Linux/macOS and Windows and supports
Python 3.7+.

.. code-block:: bash

    $ pip install wurm

First steps
-----------

To get started with Wurm, let's first create a table:

.. literalinclude:: getting_started.py
   :lines: 6-13
   :linenos:

Alright, so this tells Wurm what a ``NamedPoint`` is, that it has two
regular fields named ``x`` and ``y`` which should both be integers, and
a field named ``name`` which has a :data:`~wurm.Unique` constraint and should
be a string.

To anything interesting with it, we should connect to a database,
though. SQL databases are usually stored in a file, but if we pass
``':memory:'`` as the filename, sqlite creates a temporary database in
RAM, which is useful for quick tests and trying things out.

.. literalinclude:: getting_started.py
   :lines: 16-19
   :linenos:
   :lineno-start: 9

Now, we can create objects, insert them in the database, and try some
simple queries:

.. literalinclude:: getting_started.py
   :lines: 22-33
   :linenos:
   :lineno-start: 13

Which produces the following output:

.. literalinclude:: getting_started.py
   :lines: 37-40

TODO: explain commit, delete, queries /w comparators,
show errors from Unique constraint violations and other errors
