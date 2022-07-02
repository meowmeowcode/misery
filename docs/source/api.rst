.. _api:

API Reference
=============

Classes
-------

* :class:`misery.F`
* :exc:`misery.NotFound`
* :exc:`misery.QueryError`
* :class:`misery.Repo`
* :class:`misery.TransactionManager`
* :class:`misery.dictionary.DictRepo`
* :class:`misery.dictionary.DictTransactionManager`
* :class:`misery.postgres.PostgresRepo`
* :class:`misery.postgres.PostgresTransactionManager`
* :class:`misery.clickhouse.ClickHouseRepo`
* :class:`misery.clickhouse.ClickHouseTransactionManager`

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

.. autoclass:: QueryError
    :members:


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


ClickHouse
----------

.. module:: misery.clickhouse

.. autoclass:: ClickHouseRepo
    :members:

.. autoclass:: ClickHouseTransactionManager
    :members:
