from __future__ import annotations

import copy
import operator
import re
from abc import abstractmethod
from functools import reduce
from functools import total_ordering
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import TypeVar

from .core import F, FilterType
from .core import NotFound


T = TypeVar("T")


@total_ordering
class _Desc:
    def __init__(self, value: Any):
        self.value = value

    def __lt__(self, other: Any) -> bool:
        return self.value > other.value

    def __eq__(self, other: Any) -> bool:
        return self.value == other.value


class DictRepo(Generic[T]):
    """Implementation of the :class:`misery.Repo` protocol.
    It uses a dictionary to store entities.
    """

    id_field = "id"
    """Name of the ID field of an entity."""

    @property
    @abstractmethod
    def key(self) -> str:
        """Key inside the ``storage`` dictionary.
        The value of this key is also a dictionary.
        The repository uses this nested dictionary
        to save entities.
        """
        ...

    def __init__(self, storage: dict) -> None:
        """:param storage: Dictionary to use as a storage space.
        A storage space can be shared between multiple repositories.
        """
        self.storage = storage
        self.storage[self.key] = self.storage.get(self.key, {})

    @property
    def data(self) -> dict[Any, T]:
        """Nested dictionary inside the ``storage`` dictionary.
        The repository uses this nested dictionary
        to save entities.
        """
        return self.storage[self.key]

    async def add(self, entity: T) -> None:
        self.data[getattr(entity, self.id_field)] = copy.deepcopy(entity)

    async def add_many(self, entities: Iterable[T]) -> None:
        for entity in entities:
            await self.add(entity)

    async def get(self, **kwargs: Any) -> T:
        for entity in self.data.values():
            if self._matches(entity, **kwargs):
                return copy.deepcopy(entity)

        raise NotFound

    async def get_for_update(self, **kwargs: Any) -> T:
        return await self.get(**kwargs)

    def _matches(self, entity: T, **kwargs: Any) -> bool:
        return all(getattr(entity, k) == v for k, v in kwargs.items())

    async def get_many(
        self,
        filters: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        result = self._apply_filters(self.data.values(), filters)

        def sorting_key(x: Any) -> tuple:
            return tuple(
                _Desc(self._get_field(x, f[1:]))
                if f.startswith("-")
                else self._get_field(x, f)
                for f in order
            )

        result = sorted(result, key=sorting_key)

        if limit is not None:
            result = result[(page - 1) * limit : page * limit]

        return map(copy.deepcopy, result)

    _FILTERS_MAP = {
        FilterType.EQ: operator.eq,
        FilterType.NEQ: operator.ne,
        FilterType.LT: operator.lt,
        FilterType.LTE: operator.le,
        FilterType.GT: operator.gt,
        FilterType.GTE: operator.ge,
        FilterType.CONTAINS: lambda s, v: v in s,
        FilterType.NCONTAINS: lambda s, v: v not in s,
        FilterType.ICONTAINS: lambda s, v: v.lower() in s.lower(),
        FilterType.NICONTAINS: lambda s, v: v.lower() not in s.lower(),
        FilterType.STARTSWITH: lambda s, v: s.startswith(v),
        FilterType.NSTARTSWITH: lambda s, v: not s.startswith(v),
        FilterType.ISTARTSWITH: lambda s, v: s.lower().startswith(v.lower()),
        FilterType.NISTARTSWITH: lambda s, v: not s.lower().startswith(v.lower()),
        FilterType.ENDSWITH: lambda s, v: s.endswith(v),
        FilterType.NENDSWITH: lambda s, v: not s.endswith(v),
        FilterType.IENDSWITH: lambda s, v: s.lower().endswith(v.lower()),
        FilterType.NIENDSWITH: lambda s, v: not s.lower().endswith(v.lower()),
        FilterType.IN: lambda s, v: s in v,
        FilterType.NIN: lambda s, v: s not in v,
        FilterType.MATCHES: lambda s, v: bool(re.match(v, s)),
        FilterType.NMATCHES: lambda s, v: not re.match(v, s),
        FilterType.IMATCHES: lambda s, v: bool(re.match(v, s, re.IGNORECASE)),
        FilterType.NIMATCHES: lambda s, v: not re.match(v, s, re.IGNORECASE),
    }

    def _filter_to_op(self, f: F) -> Callable:
        return self._FILTERS_MAP[f.type]

    def _get_field(self, obj: Any, field: str) -> Any:
        return reduce(lambda x, y: getattr(x, y), field.split("."), obj)

    def _apply_filters(self, entities: Iterable[T], filters: Sequence[F]) -> list[T]:
        result = list(entities)

        for f in filters:
            result = [x for x in result if self._check(f, x)]

        return result

    def _check(self, filter_: F, entity: T) -> bool:
        if filter_.type == FilterType.OR:
            return any(self._check(ff, entity) for ff in filter_.value)
        elif filter_.type == FilterType.AND:
            return all(self._check(ff, entity) for ff in filter_.value)
        else:
            op = self._filter_to_op(filter_)
            return op(self._get_field(entity, filter_.field), filter_.value)

    async def get_first(
        self, filters: Sequence[F] = (), order: Sequence[str] = ()
    ) -> T:
        try:
            return list(await self.get_many(filters, order=order, limit=1))[0]
        except IndexError:
            raise NotFound

    async def update(self, entity: T) -> None:
        id = getattr(entity, self.id_field)
        if id not in self.data:
            raise NotFound
        self.data[id] = copy.deepcopy(entity)

    async def delete(self, **kwargs: Any) -> None:
        for id, item in list(self.data.items()):
            if self._matches(item, **kwargs):
                del self.data[id]

    async def exists(self, **kwargs: Any) -> bool:
        return any(self._matches(entity, **kwargs) for entity in self.data.values())

    async def count(self, **kwargs: Any) -> int:
        if kwargs:
            return sum(
                1 for entity in self.data.values() if self._matches(entity, **kwargs)
            )
        return len(self.data)


class DictTransactionManager:
    """Implementation of the :class:`misery.TransactionManager` protocol.
    It uses a dictionary to store entities.
    """

    def __init__(self, storage: dict) -> None:
        self._storage = storage
        self._copy: dict = {}

    async def __aenter__(self) -> None:
        self._copy = copy.deepcopy(self._storage)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is not None:
            self._storage.clear()
            self._storage.update(self._copy)
