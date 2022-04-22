from __future__ import annotations

import enum
from contextlib import AbstractAsyncContextManager
from enum import Enum
from functools import partial
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Protocol
from typing import Sequence
from typing import TypeVar


class FilterType(Enum):
    EQ = enum.auto()
    NEQ = enum.auto()
    LT = enum.auto()
    LTE = enum.auto()
    GT = enum.auto()
    GTE = enum.auto()
    CONTAINS = enum.auto()
    ICONTAINS = enum.auto()
    NCONTAINS = enum.auto()
    NICONTAINS = enum.auto()
    STARTSWITH = enum.auto()
    ISTARTSWITH = enum.auto()
    NSTARTSWITH = enum.auto()
    NISTARTSWITH = enum.auto()
    ENDSWITH = enum.auto()
    NENDSWITH = enum.auto()
    IENDSWITH = enum.auto()
    NIENDSWITH = enum.auto()
    MATCHES = enum.auto()
    IMATCHES = enum.auto()
    NMATCHES = enum.auto()
    NIMATCHES = enum.auto()
    IN = enum.auto()
    NIN = enum.auto()


class F:
    def __init__(self, type: FilterType, field: str, value: Any) -> None:
        self.type = type
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"F(type='{self.type}', field='{self.field}', value='{self.value}'')"

    @classmethod
    def eq(cls, field: str, value: Any) -> F:
        return cls(FilterType.EQ, field, value)

    @classmethod
    def lt(cls, field: str, value: Any) -> F:
        return cls(FilterType.LT, field, value)

    @classmethod
    def lte(cls, field: str, value: Any) -> F:
        return cls(FilterType.LTE, field, value)

    @classmethod
    def gt(cls, field: str, value: Any) -> F:
        return cls(FilterType.GT, field, value)

    @classmethod
    def gte(cls, field: str, value: Any) -> F:
        return cls(FilterType.GTE, field, value)

    @classmethod
    def neq(cls, field: str, value: Any) -> F:
        return cls(FilterType.NEQ, field, value)

    @classmethod
    def contains(cls, field: str, value: Any) -> F:
        return cls(FilterType.CONTAINS, field, value)

    @classmethod
    def icontains(cls, field: str, value: str) -> F:
        return cls(FilterType.ICONTAINS, field, value)

    @classmethod
    def ncontains(cls, field: str, value: Any) -> F:
        return cls(FilterType.NCONTAINS, field, value)

    @classmethod
    def nicontains(cls, field: str, value: str) -> F:
        return cls(FilterType.NICONTAINS, field, value)

    @classmethod
    def startswith(cls, field: str, value: str) -> F:
        return cls(FilterType.STARTSWITH, field, value)

    @classmethod
    def istartswith(cls, field: str, value: str) -> F:
        return cls(FilterType.ISTARTSWITH, field, value)

    @classmethod
    def nstartswith(cls, field: str, value: str) -> F:
        return cls(FilterType.NSTARTSWITH, field, value)

    @classmethod
    def nistartswith(cls, field: str, value: str) -> F:
        return cls(FilterType.NISTARTSWITH, field, value)

    @classmethod
    def endswith(cls, field: str, value: str) -> F:
        return cls(FilterType.ENDSWITH, field, value)

    @classmethod
    def nendswith(cls, field: str, value: str) -> F:
        return cls(FilterType.NENDSWITH, field, value)

    @classmethod
    def iendswith(cls, field: str, value: str) -> F:
        return cls(FilterType.IENDSWITH, field, value)

    @classmethod
    def niendswith(cls, field: str, value: str) -> F:
        return cls(FilterType.NIENDSWITH, field, value)

    @classmethod
    def matches(cls, field: str, value: str) -> F:
        return cls(FilterType.MATCHES, field, value)

    @classmethod
    def imatches(cls, field: str, value: str) -> F:
        return cls(FilterType.IMATCHES, field, value)

    @classmethod
    def nmatches(cls, field: str, value: str) -> F:
        return cls(FilterType.NMATCHES, field, value)

    @classmethod
    def nimatches(cls, field: str, value: str) -> F:
        return cls(FilterType.NIMATCHES, field, value)

    @classmethod
    def in_(cls, field: str, value: Any) -> F:
        return cls(FilterType.IN, field, value)

    @classmethod
    def nin(cls, field: str, value: Any) -> F:
        return cls(FilterType.NIN, field, value)


T = TypeVar("T")


class Repo(Protocol[T]):
    async def add(self, entity: T) -> None:
        ...

    async def get(self, **kwargs) -> T:
        ...

    async def get_for_update(self, **kwargs) -> T:
        ...

    async def get_many(
        self,
        filter_: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        ...

    async def update(self, entity: T) -> None:
        ...

    async def delete(self, **kwargs) -> None:
        ...

    async def exists(self, **kwargs) -> bool:
        ...

    async def count(self, **kwargs) -> int:
        ...


class NotFound(Exception):
    pass


TransactionManager = AbstractAsyncContextManager
