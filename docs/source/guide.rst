.. _guide:


Guide
=====

In examples on this page we’re going to use PostgreSQL because historically it is the first database supported by **misery**.

Installation
------------

At first you need to install **misery** itself:

.. code-block:: bash

    $ pip install misery

If you're going to use **misery** with PostgreSQL, you also need to install
**aysncpg** and **PyPika**:

.. code-block:: bash

    $ pip install asyncpg pypika

Creating a repository
---------------------

Let's imagine that in the business logic of your application
you have an entity like this::

    from dataclasses import dataclass
    from uuid import UUID

    @dataclass
    class User:
        id: UUID
        name: str

This entity needs to be stored somewhere. Let's create a table
in your PostgreSQL database for this purpose::

    import asyncpg

    conn = await asyncpg.connect("postgresql://postgres:password@localhost/postgres")

    await conn.execute(
        """
            CREATE TABLE users (
                id uuid PRIMARY KEY,
                name text NOT NULL UNIQUE
            );
        """
    )


When we have an entity and a table to store it, we can define a repository::

    from misery.postgres import PGRepo

    class UsersRepo(PGRepo[User]):
        table_name = "users"

To use the repository we need to instantiate it::

    users_repo = UsersRepo(conn)

Take a notice that we passed the connection to the database
to the repository. The repository will use this connection for making
queries to the database. There is no need to create a new connection every time
you want to create a repository because one connection can be shared by many repositories.

Saving entities
---------------

When the repository is ready, we can populate it with entities::

    from uuid import uuid4

    bob = User(id=uuid4(), name="Bob")
    await users_repo.add(bob)

    john = User(id=uuid4(), name="john")
    await users_repo.add(john)

Let's update the second user to fix a typo in his name::

    john.name = john.name.capitalize()
    await users_repo.update(john)

Loading entities
----------------

The simplest way to load an entity is to get it by ID::

    user = await users_repo.get(id=bob.id)
    assert user == bob

Other attributes also can be used::

    user = await users_repo.get(name="Bob")
    assert user == bob

It's possible to get many entities at once::

    users = await users_repo.get_many()
    assert len(list(users)) == 2

Entities can be ordered::

    users = await users_repo.get_many(order=["name"])
    assert [u.name for u in users] == ["Bob", "John"]

Descending ordering is also possible::

    users = await users_repo.get_many(order=["-name"])
    assert [u.name for u in users] == ["John", "Bob"]

If you don't want to load an entire collection of entities
from your database, use different types of filters::

    from misery import F

    bert = User(id=uuid4(), name="Bert")
    await users_repo.add(bert)

    users = await users_repo.get_many(filters=[F.startswith("name", "B")])
    assert set(u.name for u in users) == {"Bob", "Bert"}

To know more about filters read the API documentation.

Removing entities
-----------------

It is easy::

    await users_repo.delete(id=bert.id)

Transactions
------------

There is a special object for transactions.
Just create it and use as a context manager::

    from misery.postgres import PGTransactionManager

    transaction_manager = PGTransactionManager(conn)

    async with transaction_manager:
        await users_repo.add(User(id=uuid4(), name="Mike"))
        await users_repo.add(User(id=uuid4(), name="Mike"))

The transaction above will be rolled back due to the uniqueness
constrant on the "name" column.

Repository customization
------------------------

