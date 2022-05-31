.. _api:

API Reference
=============

Classes
-------

* :class:`misery.F`
* :exc:`misery.NotFound`
* :class:`misery.Repo`
* :class:`misery.TransactionManager`
* :class:`misery.dictionary.DictRepo`
* :class:`misery.dictionary.DictTransactionManager`
* :class:`misery.postgres.PostgresRepo`
* :class:`misery.postgres.PostgresTransactionManager`

Core
----

.. module:: misery

.. autoclass:: Repo()
    :members:

.. autoclass:: F
    :members:

.. autoclass:: TransactionManager()
    :special-members: __aenter__, __aexit__

.. autoclass:: NotFound


Dictionary
----------

.. module:: misery.dictionary

.. autoclass:: DictRepo
    :members:

.. autoclass:: DictTransactionManager
    :members:


PostgreSQL
----------

.. module:: misery.postgres

.. autoclass:: PostgresRepo
    :members:

.. autoclass:: PostgresTransactionManager
    :members:
