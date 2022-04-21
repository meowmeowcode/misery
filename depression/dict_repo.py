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
    id_field = "id"

    @property
    @abstractmethod
    def key(self) -> str:
        ...

    def __init__(self, storage: dict) -> None:
        self.storage = storage
        self.storage[self.key] = {}

    @property
    def _data(self) -> dict[Any, T]:
        return self.storage[self.key]

    async def add(self, entity: T) -> None:
        self._data[getattr(entity, self.id_field)] = copy.deepcopy(entity)

    async def get(self, **kwargs) -> T:
        for entity in self._data.values():
            if self._matches(entity, **kwargs):
                return copy.deepcopy(entity)

        raise NotFound

    async def get_for_update(self, **kwargs) -> T:
        return await self.get(**kwargs)

    def _matches(self, entity, **kwargs) -> bool:
        return all(getattr(entity, k) == v for k, v in kwargs.items())

    async def get_many(
        self,
        filter_: Sequence[F] = (),
        order: Sequence[str] = (),
        limit: Optional[int] = None,
        page: int = 1,
    ) -> Iterable[T]:
        result = list(self._data.values())

        for f in filter_:
            op = self._filter_to_op(f)
            result = [x for x in result if op(self._get_field(x, f.field), f.value)]

        def sorting_key(x):
            t = tuple(
                _Desc(self._get_field(x, f[1:]))
                if f.startswith("-")
                else self._get_field(x, f)
                for f in order
            )
            return t

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

    def _get_field(self, obj: Any, field) -> Any:
        return reduce(lambda x, y: getattr(x, y), field.split("."), obj)

    async def update(self, entity: T) -> None:
        id = getattr(entity, self.id_field)
        if id not in self._data:
            raise NotFound
        self._data[id] = copy.deepcopy(entity)

    async def delete(self, **kwargs) -> None:
        for id, item in list(self._data.items()):
            if self._matches(item, **kwargs):
                del self._data[id]

    async def exists(self, **kwargs) -> bool:
        return any(self._matches(entity, **kwargs) for entity in self._data.values())

    async def count(self, **kwargs) -> int:
        if kwargs:
            return sum(
                1 for entity in self._data.values() if self._matches(entity, **kwargs)
            )
        return len(self._data)


class DictTransactionManager:
    def __init__(self, storage: dict) -> None:
        self._storage = storage
        self._copy: dict = {}

    async def __aenter__(self) -> None:
        self._copy = copy.deepcopy(self._storage)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is not None:
            self._storage.clear()
            self._storage.update(self._copy)
