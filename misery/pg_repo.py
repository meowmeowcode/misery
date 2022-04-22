import dataclasses
import typing
from abc import abstractmethod
from contextvars import ContextVar
from enum import Enum
from typing import Any
from typing import Generic
from typing import Iterable
from typing import Optional
from typing import Protocol
from typing import Sequence
from typing import TypeVar

from asyncpg import Connection  # type: ignore
from pypika import Order  # type: ignore
from pypika import PostgreSQLQuery  # type: ignore
from pypika import Table  # type: ignore
from pypika import functions as fn  # type: ignore
from pypika.terms import BasicCriterion  # type: ignore
from pypika.terms import Term  # type: ignore
from pypika.enums import Comparator  # type: ignore

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


class PGRepo(Generic[T]):
    id_field = "id"

    @property
    @abstractmethod
    def table_name(self) -> str:
        ...

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    @property
    def table(self) -> Table:
        return Table(self.table_name)

    @property
    def query(self) -> PostgreSQLQuery:
        return PostgreSQLQuery.from_(self.table).select("*")

    @property
    def _entity_type(self) -> type:
        return typing.get_args(self.__orig_bases__[0])[0]  # type: ignore

    def load(self, record: dict) -> T:
        return self._entity_type(**record)

    def dump(self, entity: T) -> dict:
        return dataclasses.asdict(entity)

    async def execute(self, query: PostgreSQLQuery) -> None:
        await self._conn.execute(str(query))

    async def fetch_one(self, query: PostgreSQLQuery) -> T:
        data = await self._conn.fetchrow(str(query))

        if data is None:
            raise NotFound

        return self.load(dict(data))

    async def fetch_many(self, query: PostgreSQLQuery) -> Iterable[T]:
        records = await self._conn.fetch(str(query))
        return map(lambda x: self.load(dict(x)), records)

    async def fetch_value(self, query: PostgreSQLQuery) -> Any:
        return await self._conn.fetchval(str(query))

    async def add(self, entity: T) -> None:
        data = self.dump(entity)
        query = (
            PostgreSQLQuery.into(self.table)
            .columns(*data.keys())
            .insert(*data.values())
        )
        await self.execute(query)

    async def get(self, **kwargs) -> T:
        query = self.query.limit(1)

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        return await self.fetch_one(query)

    async def get_for_update(self, **kwargs) -> T:
        query = (
            PostgreSQLQuery.from_(self.table)
            .select(self.table[self.id_field])
            .for_update()
        )

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        await self._conn.fetchrow(str(query))
        return await self.get(**kwargs)

    async def get_many(
        self,
        filter_: Sequence[F] = (),
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

        for f in filter_:
            column = self.table[f.field]

            if f.type == FilterType.EQ:
                criterion = column == f.value
            elif f.type == FilterType.NEQ:
                criterion = column != f.value
            elif f.type == FilterType.LT:
                criterion = column < f.value
            elif f.type == FilterType.GT:
                criterion = column > f.value
            elif f.type == FilterType.LTE:
                criterion = column <= f.value
            elif f.type == FilterType.GTE:
                criterion = column >= f.value
            elif f.type == FilterType.STARTSWITH:
                criterion = column.like(f"{f.value}%")
            elif f.type == FilterType.NSTARTSWITH:
                criterion = column.not_like(f"{f.value}%")
            elif f.type == FilterType.ENDSWITH:
                criterion = column.like(f"%{f.value}")
            elif f.type == FilterType.NENDSWITH:
                criterion = column.not_like(f"%{f.value}")
            elif f.type == FilterType.ISTARTSWITH:
                criterion = column.ilike(f"{f.value}%")
            elif f.type == FilterType.NISTARTSWITH:
                criterion = column.not_ilike(f"{f.value}%")
            elif f.type == FilterType.IENDSWITH:
                criterion = column.ilike(f"%{f.value}")
            elif f.type == FilterType.NIENDSWITH:
                criterion = column.not_ilike(f"%{f.value}")
            elif f.type == FilterType.CONTAINS:
                criterion = column.like(f"%{f.value}%")
            elif f.type == FilterType.NCONTAINS:
                criterion = column.not_like(f"%{f.value}%")
            elif f.type == FilterType.ICONTAINS:
                criterion = column.ilike(f"%{f.value}%")
            elif f.type == FilterType.NICONTAINS:
                criterion = column.not_ilike(f"%{f.value}%")
            elif f.type == FilterType.IN:
                criterion = column.isin(f.value)
            elif f.type == FilterType.NIN:
                criterion = column.notin(f.value)
            elif f.type == FilterType.MATCHES:
                criterion = _Regexp(column, f.value)
            elif f.type == FilterType.NMATCHES:
                criterion = _NotRegexp(column, f.value)
            elif f.type == FilterType.IMATCHES:
                criterion = _IRegexp(column, f.value)
            elif f.type == FilterType.NIMATCHES:
                criterion = _NotIRegexp(column, f.value)

            query = query.where(criterion)

        return await self.fetch_many(query)

    async def update(self, entity: T) -> None:
        query = PostgreSQLQuery.update(self.table)
        record = self.dump(entity)

        for k, v in record.items():
            query = query.set(k, v)

        query = query.where(self.table[self.id_field] == getattr(entity, self.id_field))
        result = await self._conn.execute(str(query))

        if result == "UPDATE 0":
            raise NotFound

    async def delete(self, **kwargs) -> None:
        query = PostgreSQLQuery.from_(self.table).delete()

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        await self.execute(query)

    async def exists(self, **kwargs) -> bool:
        query = PostgreSQLQuery.from_(self.table).select(1).limit(1)

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        data = await self.fetch_value(query)
        return data is not None

    async def count(self, **kwargs) -> int:
        query = PostgreSQLQuery.from_(self.table).select(fn.Count("*"))

        for k, v in kwargs.items():
            query = query.where(self.table[k] == v)

        return await self.fetch_value(query)


_current_transaction = ContextVar("_current_transaction", default=None)


class PGTransactionManager:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        t = self._conn.transaction()
        await t.__aenter__()
        _current_transaction.set(t)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        t = _current_transaction.get()

        if t is not None:
            await t.__aexit__(exc_type, exc, tb)
            _current_transaction.set(None)
