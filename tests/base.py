from __future__ import annotations

import enum
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from functools import total_ordering
from typing import (
    Any,
    Optional,
)

from misery import Repo


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


SymptomsRepo = Repo[Symptom]


@dataclass
class Website:
    id: int
    address: str
    hosts: list[str] = field(default_factory=list)
    framework: Optional[str] = None


WebsitesRepo = Repo[Website]
