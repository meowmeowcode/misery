<p align="center">
    <img src="https://github.com/meowmeowcode/misery/blob/clickhouse/docs/source/_static/misery.png" width="200" alt="misery" />
</p>


# Misery

An **asyncio**-friendly database toolkit that works well with **MyPy**.

## Supported database systems

At the moment, PostgreSQL and ClickHouse are supported.

## Documentation

The latest documentation: https://misery.readthedocs.io

## Usage example

```python
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
from pypika import Table
from misery.postgres import PostgresRepo


conn = await asyncpg.connect("postgresql://postgres:password@localhost/postgres")

await conn.execute(
    """
        CREATE TABLE users (
            id uuid PRIMARY KEY,
            name text NOT NULL UNIQUE
        );
    """
)


@dataclass
class User:
    id: UUID
    name: str


class UsersRepo(PostgresRepo[User]):
    table = Table("users")


users_repo = UsersRepo(conn)

user_id = uuid4()
bob = User(id=user_id, name="Bob")
await users_repo.add(bob)

user = await users_repo.get(id=user_id)
assert user == bob
```