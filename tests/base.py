import enum
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from typing import Any

from depression.core import Repo


@total_ordering
class SymptomType(str, Enum):
    PSYCHOLOGICAL = "PSYCHOLOGICAL"
    PHYSICAL = "PHYSICAL"

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, SymptomType):
            return self.name < other.name
        return NotImplemented


@dataclass
class Symptom:
    id: int
    name: str
    type: SymptomType


class SymptomsRepo(Repo[Symptom]):
    pass
