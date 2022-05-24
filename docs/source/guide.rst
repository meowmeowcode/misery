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

    from pypika import Table
    from misery.postgres import PGRepo

    class UsersRepo(PGRepo[User]):
        table = Table("users")

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

    users = await users_repo.get_many([F.startswith("name", "B")])
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

The default behaviour is not enough when things get more complex
and some additional code has to be written. Look what
may change in the case of one-to-many relationship::

    from typing import List

    from pypika import Parameter, PostgreSQLQuery
    from pypika.terms import AggregateFunction


    @dataclass
    class User:
        id: UUID
        name: str
        emails: List[str]


    await conn.execute(
        """
            CREATE TABLE emails (
                id uuid PRIMARY KEY,
                email text NOT NULL UNIQUE,
                user_id uuid REFERENCES users(id)
            );
        """
    )


    class UsersRepo(PGRepo[User]):
        table = Table("users")
        emails_table = Table("emails")

        query = PostgreSQLQuery.from_(
            table
        ).left_outer_join(
            emails_table
        ).on(
            emails_table.user_id == table.id
        ).groupby(
            table.id,
            table.name,
        ).select(
            table.id,
            table.name,
            AggregateFunction(
                "array_agg",
                emails_table.email,
            ).as_("emails")
        )

        def dump(self, entity: User) -> dict:
            return {
                "id": entity.id,
                "name": entity.name,
            }

        def load(self, row: dict) -> User:
            return User(
                id=row["id"],
                name=row["name"],
                emails=[
                    x for x in row["emails"]
                    if x is not None
                ],
            )

        async def after_add(self, entity: User) -> None:
            await self._save_emails(entity)

        async def _save_emails(self, entity: User) -> None:
            query = (
                PostgreSQLQuery.into(self.emails_table)
                .columns("id", "email", "user_id")
                .insert(
                    Parameter("$1"),
                    Parameter("$2"),
                    Parameter("$3")
                )
            )

            await self.conn.executemany(
                str(query),
                ((uuid4(), e, entity.id) for e in entity.emails)
            )

        async def after_update(self, entity: User) -> None:
            # For simplicity,
            # let's just delete all previous email rows
            query = PostgreSQLQuery.from_(
                self.emails_table
            ).delete().where(
                self.emails_table.user_id == entity.id
            )
            await self.conn.execute(str(query))

            await self._save_emails(entity)

    users_repo = UsersRepo(conn)
    bob = await users_repo.get(name="Bob")
    bob.emails = ["bob@test.com", "bobmail@test.com"]
    await users_repo.update(bob)
    john = await users_repo.get(name="John")
    john.emails = ["john@test.com"]
    await users_repo.update(john)
    user = await users_repo.get(id=bob.id)
    assert user.emails == bob.emails


Fast prototyping
----------------
Sometimes when you're making a prototype
or writing tests of the business logic
the database schema may be unimportant at all.
In this case instead of doing potentially useless job
you can use a dictionary-based repository to store
entities::

    from misery.dictionary import DictRepo

    data = {}

    class UsersRepo(DictRepo[User]):
        key = "users"

    users_repo = UsersRepo(data)

In this example the "data" dictionary will be used
instead of a database. The "key" attribute of a repository
serves like a name of a table to keep entities
of different types separately.

The dictionary-based repository implements the same protocol
as the PostgreSQL-based one, so it can be replacad by it
whenever you're ready.
