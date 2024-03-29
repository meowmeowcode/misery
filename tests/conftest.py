import asyncio
from typing import Generator

import pytest  # type: ignore

from .base import (
    Symptom,
    SymptomType,
    SymptomsRepo,
    Website,
    WebsitesRepo,
)


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def hopelessness(symptoms_repo: SymptomsRepo) -> Symptom:
    s = Symptom(id=1, name="Hopelessness", type=SymptomType.PSYCHOLOGICAL)
    await symptoms_repo.add(s)
    return s


@pytest.fixture
async def helplessness(symptoms_repo: SymptomsRepo) -> Symptom:
    s = Symptom(id=2, name="Helplessness", type=SymptomType.PSYCHOLOGICAL)
    await symptoms_repo.add(s)
    return s


@pytest.fixture
async def constipation(symptoms_repo: SymptomsRepo) -> Symptom:
    s = Symptom(id=4, name="Constipation", type=SymptomType.PHYSICAL)
    await symptoms_repo.add(s)
    return s


@pytest.fixture
async def insomnia(symptoms_repo: SymptomsRepo) -> Symptom:
    s = Symptom(id=3, name="Insomnia", type=SymptomType.PHYSICAL)
    await symptoms_repo.add(s)
    return s


@pytest.fixture
async def website(websites_repo: WebsitesRepo) -> Website:
    w = Website(id=1, address="192.168.1.5")
    await websites_repo.add(w)
    return w


@pytest.fixture
async def website2(websites_repo: WebsitesRepo) -> Website:
    w = Website(
        id=2,
        address="192.168.2.5",
        hosts=["test2.com", "test2.test2.com"],
        framework="Django",
    )
    await websites_repo.add(w)
    return w
