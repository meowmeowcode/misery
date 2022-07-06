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
from typing import Union
from typing import ForwardRef


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
    OR = enum.auto()
    AND = enum.auto()


class F:
    """Object used for filtering entities
    before getting them from a repository."""

    def __init__(self, type: FilterType, field: str, value: Any) -> None:
        self.type = type
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"F(type='{self.type}', field='{self.field}', value='{self.value}'')"

    @classmethod
    def eq(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a specified field value.
        """
        return cls(FilterType.EQ, field, value)

    @classmethod
    def lt(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a field value that is
        less than a given one.
        """
        return cls(FilterType.LT, field, value)

    @classmethod
    def lte(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a field value that is
        less than or equal to a given one.
        """
        return cls(FilterType.LTE, field, value)

    @classmethod
    def gt(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a field value that is
        greater than a given one.
        """
        return cls(FilterType.GT, field, value)

    @classmethod
    def gte(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a field value that is
        greater than or equal to a given one.
        """
        return cls(FilterType.GTE, field, value)

    @classmethod
    def neq(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        with a field value that is
        not equal to a given one.
        """
        return cls(FilterType.NEQ, field, value)

    @classmethod
    def contains(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        whose field contains a given value.

        Case-sensitive.
        """
        return cls(FilterType.CONTAINS, field, value)

    @classmethod
    def icontains(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field contains a given value.

        Case-insensitive.
        """
        return cls(FilterType.ICONTAINS, field, value)

    @classmethod
    def ncontains(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        whose field doesn't contain a given value.

        Case-sensitive.
        """
        return cls(FilterType.NCONTAINS, field, value)

    @classmethod
    def nicontains(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field doesn't contain a given value.

        Case-insensitive.
        """
        return cls(FilterType.NICONTAINS, field, value)

    @classmethod
    def startswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field starts with a given value.

        Case-sensitive.
        """
        return cls(FilterType.STARTSWITH, field, value)

    @classmethod
    def istartswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field starts with a given value.

        Case-insensitive.
        """
        return cls(FilterType.ISTARTSWITH, field, value)

    @classmethod
    def nstartswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field doesn't start with a given value.

        Case-sensitive.
        """
        return cls(FilterType.NSTARTSWITH, field, value)

    @classmethod
    def nistartswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field doesn't start with a given value.

        Case-insensitive.
        """
        return cls(FilterType.NISTARTSWITH, field, value)

    @classmethod
    def endswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field ends with a given value.

        Case-sensitive.
        """
        return cls(FilterType.ENDSWITH, field, value)

    @classmethod
    def nendswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field doesn't end with a given value.

        Case-sensitive.
        """
        return cls(FilterType.NENDSWITH, field, value)

    @classmethod
    def iendswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field value ends with a given value.

        Case-insensitive.
        """
        return cls(FilterType.IENDSWITH, field, value)

    @classmethod
    def niendswith(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field value doesn't end with a given value.

        Case-insensitive.
        """
        return cls(FilterType.NIENDSWITH, field, value)

    @classmethod
    def matches(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field matches
        a regular expression in the ``value`` parameter.

        Case-sensitive.
        """
        return cls(FilterType.MATCHES, field, value)

    @classmethod
    def imatches(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field value matches
        a regular expression in the ``value`` parameter.

        Case-insensitive.
        """
        return cls(FilterType.IMATCHES, field, value)

    @classmethod
    def nmatches(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field value doesn't match
        a regular expression in the ``value`` parameter.

        Case-sensitive.
        """
        return cls(FilterType.NMATCHES, field, value)

    @classmethod
    def nimatches(cls, field: str, value: str) -> F:
        """Create a filter to find entities
        whose field value doesn't match
        a regular expression in the ``value`` parameter.

        Case-insensitive.
        """
        return cls(FilterType.NIMATCHES, field, value)

    @classmethod
    def in_(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        whose field is within a given value.

        Case-sensitive.
        """
        return cls(FilterType.IN, field, value)

    @classmethod
    def nin(cls, field: str, value: Any) -> F:
        """Create a filter to find entities
        whose field is not within a given value.

        Case-sensitive.
        """
        return cls(FilterType.NIN, field, value)

    @classmethod
    def or_(cls, *filters: F) -> F:
        """Create a filter that combines multiple
        filters with the "OR" operator.
        """
        return cls(FilterType.OR, "", filters)

    @classmethod
    def and_(cls, *filters: F) -> F:
        """Create a filter that combines multiple
        filters with the "AND" operator.
        """
        return cls(FilterType.AND, "", filters)


T = TypeVar("T")


class Repo(Protocol[T]):
    """Protocol for all repositories to implement."""

    async def add(self, entity: T) -> None:
        """Add a new entity to the repository.

        :param entity: An entity to add.

        """
        ...

    async def add_many(self, entities: Iterable[T]) -> None:
        """Add multiple entities to the repository.

        :param entities: Entities to add.
        """
        ...

    async def get(self, **kwargs: Any) -> T:
        """Get an entity.
        Raise :exc:`misery.NotFound` if the entity is missing.

        :param kwargs: Lookup parameters as field-value pairs.
        """
        ...

    async def get_for_update(self, **kwargs: Any) -> T:
        """Get an entity and lock it for update.
        Raise :exc:`misery.NotFound` if the entity is missing.

        :param kwargs: Lookup parameters as field-value pairs.
        """
        ...

    async def get_many(
        self,
        filters: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        """Get multiple entities.

        :param filters: Sequence of filters.
        :param order: Sequence of fields by which to sort entities.
            If a field starts with the "-" character,
            entities for the field will be shown in descending order.
        :param limit: Maximum number of entities to show on the page.
        :param page: Page number. Only when the ``limit`` parameter
            is used.
        """
        ...

    async def get_first(
        self, filters: Sequence[F] = (), order: Sequence[str] = ()
    ) -> T:
        """Get the first entity matching given filters.
        Raise :exc:`misery.NotFound` if the entity is missing.

        :param filters: Sequence of filters.
        :param order: Sequence of fields by which to sort entities.
            If a field starts with the "-" character,
            entities for the field will be shown in descending order.
        """
        ...

    async def update(self, entity: T) -> None:
        """Save an updated entity.
        Raise :exc:`misery.NotFound` if the entity
        is not present in the repository.

        :param entity: An updated entity.
        """
        ...

    async def delete(self, **kwargs: Any) -> None:
        """Delete entities. When ``kwargs`` is empty, delete
        all.

        :param kwargs: Lookup parameters as field-value pairs.
        """
        ...

    async def exists(self, **kwargs: Any) -> bool:
        """Check if there is an entity for given lookup parameters.

        :param kwargs: Lookup parameters as field-value pairs.
        """
        ...

    async def count(self, **kwargs: Any) -> int:
        """Count entities that match given lookup parameters.
        If there are no lookup parameters, count all entities in
        the repository.

        :param kwargs: Lookup parameters as field-value pairs.
        """
        ...


class NotFound(Exception):
    """Error to raise when an entity cannot be found
    in the repository."""

    pass


class QueryError(Exception):
    """Error to raise when a query to the database
    cannot be processed."""

    def __init__(self, message: str, query: str) -> None:
        """:param message: Message from the database system.
        :param query: Query that cannot be processed.
        """
        self.message = message
        self.query = query

    def __str__(self) -> str:
        return f"QueryError(message='{self.message}', query='{self.query}')"


class TransactionManager(Protocol):
    """Protocol of an object that is used
    for transaction management."""

    async def __aenter__(self) -> None:
        """Start a new transaction."""
        ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit or rollback a transaction."""
        ...
