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
    """An object which is used for filtering of entities
    before getting them from a repository."""

    def __init__(self, type: FilterType, field: str, value: Any) -> None:
        self.type = type
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        return f"F(type='{self.type}', field='{self.field}', value='{self.value}'')"

    @classmethod
    def eq(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        equal to a given one.
        """
        return cls(FilterType.EQ, field, value)

    @classmethod
    def lt(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        less than a given one.
        """
        return cls(FilterType.LT, field, value)

    @classmethod
    def lte(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        less than or equal to a given one.
        """
        return cls(FilterType.LTE, field, value)

    @classmethod
    def gt(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        greater than a given one.
        """
        return cls(FilterType.GT, field, value)

    @classmethod
    def gte(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        greater than or equal to a given one.
        """
        return cls(FilterType.GTE, field, value)

    @classmethod
    def neq(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a value in a ``field`` that is
        not equal to a given one.
        """
        return cls(FilterType.NEQ, field, value)

    @classmethod
    def contains(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a ``field`` that contains a ``value``.
        """
        return cls(FilterType.CONTAINS, field, value)

    @classmethod
    def icontains(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that contains a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.ICONTAINS, field, value)

    @classmethod
    def ncontains(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't contain a ``value``.
        """
        return cls(FilterType.NCONTAINS, field, value)

    @classmethod
    def nicontains(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't contain a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.NICONTAINS, field, value)

    @classmethod
    def startswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that starts with a ``value``.
        """
        return cls(FilterType.STARTSWITH, field, value)

    @classmethod
    def istartswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that starts with a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.ISTARTSWITH, field, value)

    @classmethod
    def nstartswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't start with a ``value``.
        """
        return cls(FilterType.NSTARTSWITH, field, value)

    @classmethod
    def nistartswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't start with a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.NISTARTSWITH, field, value)

    @classmethod
    def endswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that ends with a ``value``.
        """
        return cls(FilterType.ENDSWITH, field, value)

    @classmethod
    def nendswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't end with a ``value``.
        """
        return cls(FilterType.NENDSWITH, field, value)

    @classmethod
    def iendswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that ends with a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.IENDSWITH, field, value)

    @classmethod
    def niendswith(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't end with a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.NIENDSWITH, field, value)

    @classmethod
    def matches(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that matches
        a regular expression in a ``value``.
        """
        return cls(FilterType.MATCHES, field, value)

    @classmethod
    def imatches(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that matches
        a regular expression in a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.IMATCHES, field, value)

    @classmethod
    def nmatches(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't match
        a regular expression in a ``value``.
        """
        return cls(FilterType.NMATCHES, field, value)

    @classmethod
    def nimatches(cls, field: str, value: str) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't match
        a regular expression in a ``value``.

        Case-insensitive version.
        """
        return cls(FilterType.NIMATCHES, field, value)

    @classmethod
    def in_(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a ``field`` that has a value
        contained in a given one.
        """
        return cls(FilterType.IN, field, value)

    @classmethod
    def nin(cls, field: str, value: Any) -> F:
        """Make a filter to find entities
        with a ``field`` that doesn't have
        a value contained in a given one.
        """
        return cls(FilterType.NIN, field, value)


T = TypeVar("T")


class Repo(Protocol[T]):
    """The protocol which all repositories must implement."""

    async def add(self, entity: T) -> None:
        """Add a new entity to the repository.

        :param entity: An entity to add.

        """
        ...

    async def add_many(self, entities: Iterable[T]) -> None:
        """Add many entities to the repository.

        :param entities: Entities to add.
        """
        ...

    async def get(self, **kwargs) -> T:
        """Get an entity.
        Raise a :exc:`misery.NotFound` if the entity is missing.

        :param kwargs: Lookup parameters in the form of field-value pairs.
        """
        ...

    async def get_for_update(self, **kwargs) -> T:
        """Get an entity and lock it for update.
        Raise a :exc:`misery.NotFound` if the entity is missing.

        :param kwargs: Lookup parameters in the form of field-value pairs.
        """
        ...

    async def get_many(
        self,
        filters: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        """Get many entities.

        :param filters: A sequence of filters.
        :param order: A sequence of fields by which entities
            must be ordered. If an item of the sequence
            starts with the "-" character
            then the descending ordering will be applied for this field.
        :param limit: Maximum number of entities on the requested page.
        :param page: A number of page to get when the ``limit`` parameter
            is used.
        """
        ...

    async def update(self, entity: T) -> None:
        """Save an updated entity.
        Raise a :exc:`misery.NotFound` if the entity
        is not present in the repository.

        :param entity: An updated entity.
        """
        ...

    async def delete(self, **kwargs) -> None:
        """Delete entities. When ``kwargs`` are empty, delete
        everything.

        :param kwargs: Lookup parameters in the form of field-value pairs.
        """
        ...

    async def exists(self, **kwargs) -> bool:
        """Check if there is an entity for the given lookup parameters.

        :param kwargs: Lookup parameters in the form of field-value pairs.
        """
        ...

    async def count(self, **kwargs) -> int:
        """Count entities matching the given lookup parameters.
        If there is no lookup parameters, count all entities in
        the repository.

        :param kwargs: Lookup parameters in the form of field-value pairs.
        """
        ...


class NotFound(Exception):
    """An error to raise when an entity cannot be found
    in a repository."""

    pass


class TransactionManager(Protocol):
    """A protocol of an object which is used
    for management of transactions."""

    async def __aenter__(self) -> None:
        """Start a transaction."""
        ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit or rollback a transaction."""
        ...
