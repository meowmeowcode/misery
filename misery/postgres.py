import dataclasses
import itertools
import typing
from abc import abstractmethod
from contextvars import ContextVar
from enum import Enum
from typing import (
    Any,
    Generic,
    Iterable,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

from asyncpg import (  # type: ignore
    Connection,
    Pool,
)
from pypika import (  # type: ignore
    Criterion,
    Order,
    Parameter,
    PostgreSQLQuery,
    Table,
    functions as fn,
)
from pypika.enums import Comparator  # type: ignore
from pypika.terms import (  # type: ignore
    BasicCriterion,
    Term,
)

from .core import F
from .core import FilterType
from .core import NotFound


T = TypeVar("T")


class _PostgreSQLMatching(Comparator):
    regexp = " ~ "
    not_regexp = " !~ "
    iregexp = " ~* "
    not_iregexp = " !~* "


class _Regexp(BasicCriterion):
    def __init__(self, term: Term, expr: str) -> None:
        super().__init__(_PostgreSQLMatching.regexp, term, term.wrap_constant(expr))


class _NotRegexp(BasicCriterion):
    def __init__(self, term: Term, expr: str) -> None:
        super().__init__(_PostgreSQLMatching.not_regexp, term, term.wrap_constant(expr))


class _IRegexp(BasicCriterion):
    def __init__(self, term: Term, expr: str) -> None:
        super().__init__(_PostgreSQLMatching.iregexp, term, term.wrap_constant(expr))


class _NotIRegexp(BasicCriterion):
    def __init__(self, term: Term, expr: str) -> None:
        super().__init__(
            _PostgreSQLMatching.not_iregexp, term, term.wrap_constant(expr)
        )


class PostgresRepo(Generic[T]):
    """Implementation of the :class:`misery.Repo` protocol
    that uses asyncpg to communicate with PostgreSQL.
    """

    id_field = "id"
    """Name of the ID field of an entity."""

    @property
    @abstractmethod
    def table(self) -> Table:
        """Main table of the repository.
        It is used to autogenerate SQL queries.
        """
        ...

    def __init__(self, conn: Union[Connection, Pool]) -> None:
        """:param conn: Connection or pool that
        will be used for interaction with the database.
        """
        self._conn = conn

    @property
    def conn(self) -> Union[Connection, Pool]:
        """Connection or pool that is used
        for interaction with the database.

        Use this property in your custom methods.
        """
        return _current_conn.get() or self._conn

    @property
    def query(self) -> PostgreSQLQuery:
        """Query to select records from the database.

        Override this property when you need
        something more complex than selecting
        all columns from the main table
        of the repository.
        """
        return PostgreSQLQuery.from_(self.table).select("*")

    @property
    def _entity_type(self) -> type:
        return typing.get_args(self.__orig_bases__[0])[0]  # type: ignore

    def load(self, record: dict) -> T:
        """Map a database record to an entity.

        Override this method if needed.
        """
        return self._entity_type(**record)

    def dump(self, entity: T) -> dict:
        """Map an entity to a database record.

        Override this method if needed.
        """
        return dataclasses.asdict(entity)

    async def fetch_one(self, query: PostgreSQLQuery) -> T:
        """Find a record in the database
        and map it to an entity.
        """
        data = await self.conn.fetchrow(str(query))

        if data is None:
            raise NotFound

        return self.load(dict(data))

    async def fetch_many(self, query: PostgreSQLQuery) -> Iterable[T]:
        """Find multiple records in the database
        and map them to entities.
        """
        records = await self.conn.fetch(str(query))
        return map(lambda x: self.load(dict(x)), records)

    async def add(self, entity: T) -> None:
        data = self.dump(entity)
        query = (
            PostgreSQLQuery.into(self.table)
            .columns(*data.keys())
            .insert(*data.values())
        )
        await self.conn.execute(str(query))
        await self.after_add(entity)

    async def after_add(self, entity: T) -> None:
        """Action after adding a new entity.

        By default, this method doesn't do anything.
        Override it to your liking.
        """
        pass

    async def add_many(self, entities: Iterable[T]) -> None:
        ientities = iter(entities)
        first = next(ientities)
        data = self.dump(first)

        query = (
            PostgreSQLQuery.into(self.table)
            .columns(*data.keys())
            .insert(*[Parameter(f"${n}") for n in range(1, len(data) + 1)])
        )

        await self.conn.executemany(
            str(query),
            itertools.chain(
                [tuple(data.values())],
                (tuple(self.dump(x).values()) for x in ientities),
            ),
        )

    async def get(self, **kwargs: Any) -> T:
        query = self.query.limit(1)

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        return await self.fetch_one(query)

    async def get_for_update(self, **kwargs: Any) -> T:
        query = (
            PostgreSQLQuery.from_(self.table)
            .select(self.table[self.id_field])
            .for_update()
        )

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        await self.conn.fetchrow(str(query))
        return await self.get(**kwargs)

    async def get_many(
        self,
        filters: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        query = self.query

        if limit is not None:
            query = query.limit(limit).offset((page - 1) * limit)

        for field in order:
            if field.startswith("-"):
                query = query.orderby(field[1:], order=Order.desc)
            else:
                query = query.orderby(field)

        for f in filters:
            criterion = self._filter_to_criterion(f)
            query = query.where(criterion)

        return await self.fetch_many(query)

    def _filter_to_criterion(self, f: F) -> Criterion:
        if f.type == FilterType.OR:
            return Criterion.any([self._filter_to_criterion(ff) for ff in f.value])
        elif f.type == FilterType.AND:
            return Criterion.all([self._filter_to_criterion(ff) for ff in f.value])

        column = self.table[f.field]

        if f.type == FilterType.EQ:
            return column == f.value
        elif f.type == FilterType.NEQ:
            return column != f.value
        elif f.type == FilterType.LT:
            return column < f.value
        elif f.type == FilterType.GT:
            return column > f.value
        elif f.type == FilterType.LTE:
            return column <= f.value
        elif f.type == FilterType.GTE:
            return column >= f.value
        elif f.type == FilterType.STARTSWITH:
            return column.like(f"{f.value}%")
        elif f.type == FilterType.NSTARTSWITH:
            return column.not_like(f"{f.value}%")
        elif f.type == FilterType.ENDSWITH:
            return column.like(f"%{f.value}")
        elif f.type == FilterType.NENDSWITH:
            return column.not_like(f"%{f.value}")
        elif f.type == FilterType.ISTARTSWITH:
            return column.ilike(f"{f.value}%")
        elif f.type == FilterType.NISTARTSWITH:
            return column.not_ilike(f"{f.value}%")
        elif f.type == FilterType.IENDSWITH:
            return column.ilike(f"%{f.value}")
        elif f.type == FilterType.NIENDSWITH:
            return column.not_ilike(f"%{f.value}")
        elif f.type == FilterType.CONTAINS:
            return column.like(f"%{f.value}%")
        elif f.type == FilterType.NCONTAINS:
            return column.not_like(f"%{f.value}%")
        elif f.type == FilterType.ICONTAINS:
            return column.ilike(f"%{f.value}%")
        elif f.type == FilterType.NICONTAINS:
            return column.not_ilike(f"%{f.value}%")
        elif f.type == FilterType.IN:
            return column.isin(f.value)
        elif f.type == FilterType.NIN:
            return column.notin(f.value)
        elif f.type == FilterType.MATCHES:
            return _Regexp(column, f.value)
        elif f.type == FilterType.NMATCHES:
            return _NotRegexp(column, f.value)
        elif f.type == FilterType.IMATCHES:
            return _IRegexp(column, f.value)
        elif f.type == FilterType.NIMATCHES:
            return _NotIRegexp(column, f.value)

    async def get_first(
        self, filters: Sequence[F] = (), order: Sequence[str] = ()
    ) -> T:
        try:
            return list(await self.get_many(filters, order=order, limit=1))[0]
        except IndexError:
            raise NotFound

    async def update(self, entity: T) -> None:
        query = PostgreSQLQuery.update(self.table)
        record = self.dump(entity)

        for k, v in record.items():
            query = query.set(k, v)

        query = query.where(self.table[self.id_field] == getattr(entity, self.id_field))
        result = await self.conn.execute(str(query))

        if result == "UPDATE 0":
            raise NotFound

        await self.after_update(entity)

    async def after_update(self, entity: T) -> None:
        """Action after update.

        By default, this method doesn't do anything.
        Override it to your liking.
        """
        pass

    async def delete(self, **kwargs: Any) -> None:
        query = PostgreSQLQuery.from_(self.table).delete()

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        await self.conn.execute(str(query))

    async def exists(self, **kwargs: Any) -> bool:
        query = PostgreSQLQuery.from_(self.table).select(1).limit(1)

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        data = await self.conn.fetchval(str(query))
        return data is not None

    async def count(self, **kwargs: Any) -> int:
        query = PostgreSQLQuery.from_(self.table).select(fn.Count("*"))

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        return await self.conn.fetchval(str(query))


_current_transaction = ContextVar("_current_transaction", default=None)
_current_conn = ContextVar("_current_conn", default=None)


class PostgresTransactionManager:
    """Implementation of the :class:`misery.TransactionManager` protocol
    that uses asyncpg to communicate with PostgreSQL.
    """

    def __init__(self, conn: Union[Connection, Pool]) -> None:
        if isinstance(conn, Pool):
            self._pool, self._conn = conn, None
        else:
            self._pool, self._conn = None, conn

    @property
    def conn(self) -> Connection:
        if self._pool is None:
            return self._conn

        return _current_conn.get()

    async def __aenter__(self) -> None:
        if self._pool is not None:
            _current_conn.set(await self._pool.acquire())

        t = self.conn.transaction()
        await t.__aenter__()
        _current_transaction.set(t)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        t = _current_transaction.get()

        if t is not None:
            await t.__aexit__(exc_type, exc, tb)
            _current_transaction.set(None)

        if _current_conn.get() is not None:
            await self._pool.release(_current_conn.get())
            _current_conn.set(None)
