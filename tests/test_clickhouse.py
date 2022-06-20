from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    Generator,
    Sequence,
    Union,
)

import pytest  # type: ignore

from aiohttp import (
    BasicAuth,
    ClientSession,
)

from pypika import Table  # type: ignore

from misery import (
    F,
    NotFound,
    Repo,
    TransactionManager,
)

from misery.clickhouse import (
    ClickhouseRepo,
    ClickhouseTransactionManager,
)

from .base import (
    Symptom,
    SymptomType,
    SymptomsRepo,
)


class SymptomsClickhouseRepo(ClickhouseRepo[Symptom]):
    table = Table("symptoms")


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def session() -> AsyncGenerator:
    async with ClientSession(
        "http://localhost:8123", auth=BasicAuth("misery", "misery")
    ) as s:
        yield s


@pytest.fixture(scope="session", autouse=True)
async def db_schema(session: ClientSession) -> AsyncGenerator:
    async with session.post(
        "/",
        data="""
            CREATE TABLE symptoms (
                id UInt32 NOT NULL,
                name String NOT NULL,
                type String NOT NULL
            )
            ENGINE = MergeTree() ORDER BY id
        """,
    ) as resp:
        assert await resp.read() == b""

    yield

    async with session.post("/", data="DROP TABLE symptoms") as resp:
        assert await resp.read() == b""


@pytest.fixture(autouse=True)
async def clean_db(db_schema: None, session: ClientSession) -> None:
    await session.post("/", data="TRUNCATE TABLE symptoms")


@pytest.fixture
def symptoms_repo(session: ClientSession) -> SymptomsRepo:
    return SymptomsClickhouseRepo(session)


@pytest.fixture
def transaction_manager() -> TransactionManager:
    return ClickhouseTransactionManager()


async def test_add_many(symptoms_repo: SymptomsRepo) -> None:
    irritability = Symptom(id=1, name="Irritability", type=SymptomType.PSYCHOLOGICAL)
    back_pain = Symptom(id=2, name="Back pain", type=SymptomType.PHYSICAL)
    await symptoms_repo.add_many([irritability, back_pain])
    symptoms = list(await symptoms_repo.get_many())
    assert symptoms == [irritability, back_pain]


async def test_get(symptoms_repo: SymptomsRepo, hopelessness: Symptom) -> None:
    s = await symptoms_repo.get(id=hopelessness.id)
    assert s == hopelessness


async def test_get_for_update(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom
) -> None:
    s = await symptoms_repo.get_for_update(id=hopelessness.id)
    assert s == hopelessness


async def test_get_many(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    symptoms = list(await symptoms_repo.get_many())
    assert {s.id for s in symptoms} == {hopelessness.id, helplessness.id}


@pytest.mark.parametrize(
    "filters, names",
    [
        ([F.eq("name", "Insomnia")], {"Insomnia"}),
        ([F.neq("name", "Insomnia")], {"Hopelessness", "Helplessness", "Constipation"}),
        ([F.contains("name", "les")], {"Hopelessness", "Helplessness"}),
        ([F.contains("name", "LES")], set()),
        ([F.ncontains("name", "les")], {"Insomnia", "Constipation"}),
        (
            [F.ncontains("name", "LES")],
            {"Hopelessness", "Helplessness", "Insomnia", "Constipation"},
        ),
        ([F.icontains("name", "LES")], {"Hopelessness", "Helplessness"}),
        ([F.nicontains("name", "LES")], {"Insomnia", "Constipation"}),
        ([F.startswith("name", "H")], {"Hopelessness", "Helplessness"}),
        ([F.startswith("name", "h")], set()),
        ([F.nstartswith("name", "H")], {"Insomnia", "Constipation"}),
        (
            [F.nstartswith("name", "h")],
            {"Hopelessness", "Helplessness", "Insomnia", "Constipation"},
        ),
        ([F.istartswith("name", "h")], {"Hopelessness", "Helplessness"}),
        ([F.nistartswith("name", "h")], {"Insomnia", "Constipation"}),
        ([F.endswith("name", "ness")], {"Hopelessness", "Helplessness"}),
        ([F.endswith("name", "NESS")], set()),
        ([F.nendswith("name", "ness")], {"Insomnia", "Constipation"}),
        (
            [F.nendswith("name", "NESS")],
            {"Hopelessness", "Helplessness", "Insomnia", "Constipation"},
        ),
        ([F.iendswith("name", "NESS")], {"Hopelessness", "Helplessness"}),
        ([F.niendswith("name", "NESS")], {"Insomnia", "Constipation"}),
        ([F.matches("name", r"^H.*s$")], {"Hopelessness", "Helplessness"}),
        ([F.matches("name", r"^h.*s$")], set()),
        ([F.nmatches("name", r"^H.*s$")], {"Insomnia", "Constipation"}),
        (
            [F.nmatches("name", r"^h.*s$")],
            {"Hopelessness", "Helplessness", "Insomnia", "Constipation"},
        ),
        ([F.imatches("name", r"^h.*s$")], {"Hopelessness", "Helplessness"}),
        ([F.nimatches("name", r"^h.*s$")], {"Insomnia", "Constipation"}),
        ([F.in_("name", ("Insomnia", "Constipation"))], {"Insomnia", "Constipation"}),
        (
            [F.nin("name", ("Insomnia", "Constipation"))],
            {"Hopelessness", "Helplessness"},
        ),
        ([F.lt("id", 2)], {"Hopelessness"}),
        ([F.lte("id", 2)], {"Hopelessness", "Helplessness"}),
        ([F.gt("id", 3)], {"Constipation"}),
        ([F.gte("id", 3)], {"Insomnia", "Constipation"}),
    ],
)
async def test_get_many_with_filter(
    filters: Sequence[F],
    names: set[str],
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
    constipation: Symptom,
) -> None:
    symptoms = await symptoms_repo.get_many(filters)
    assert {s.name for s in symptoms} == names


@pytest.mark.parametrize(
    "order, names",
    [
        (["name"], ["Constipation", "Helplessness", "Hopelessness", "Insomnia"]),
        (["-name"], ["Insomnia", "Hopelessness", "Helplessness", "Constipation"]),
        (
            ("type", "-name"),
            ["Insomnia", "Constipation", "Hopelessness", "Helplessness"],
        ),
        (
            ("-type", "name"),
            ["Helplessness", "Hopelessness", "Constipation", "Insomnia"],
        ),
    ],
)
async def test_get_many_with_order(
    order: Sequence[str],
    names: list[str],
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    constipation: Symptom,
    insomnia: Symptom,
) -> None:
    symptoms = await symptoms_repo.get_many(order=order)
    assert [s.name for s in symptoms] == names


@pytest.mark.parametrize(
    "limit, page, names",
    [
        (2, 1, ["Hopelessness", "Helplessness"]),
        (2, 2, ["Insomnia", "Constipation"]),
        (3, 2, ["Constipation"]),
    ],
)
async def test_get_many_with_page_and_limit(
    limit: int,
    page: int,
    names: list[str],
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
    constipation: Symptom,
) -> None:
    symptoms = await symptoms_repo.get_many(limit=limit, page=page)
    assert [s.name for s in symptoms] == names


async def test_delete(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    await symptoms_repo.delete(id=hopelessness.id)
    symptoms = list(await symptoms_repo.get_many())
    assert symptoms == [helplessness]


async def test_update(symptoms_repo: SymptomsRepo, helplessness: Symptom) -> None:
    helplessness.type = SymptomType.PHYSICAL
    await symptoms_repo.update(helplessness)
    symptom = await symptoms_repo.get(id=helplessness.id)
    assert symptom == helplessness


async def test_update_not_found(
    symptoms_repo: SymptomsRepo, helplessness: Symptom
) -> None:
    helplessness.id = 123

    with pytest.raises(NotFound):
        await symptoms_repo.update(helplessness)


@pytest.mark.parametrize(
    "params, result",
    [
        ({"name": "Hopelessness"}, True),
        ({"name": "Euphoria"}, False),
    ],
)
async def test_exists(
    params: dict,
    result: bool,
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
) -> None:
    assert await symptoms_repo.exists(**params) is result


async def test_delete_all(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    await symptoms_repo.delete()
    symptoms = list(await symptoms_repo.get_many())
    assert symptoms == []


async def test_count(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
) -> None:
    assert await symptoms_repo.count(type=SymptomType.PSYCHOLOGICAL) == 2


async def test_count_all(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
) -> None:
    assert await symptoms_repo.count() == 3


async def test_transaction(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    transaction_manager: TransactionManager,
) -> None:
    async with transaction_manager:
        await symptoms_repo.delete(id=hopelessness.id)

    assert await symptoms_repo.exists(id=hopelessness.id) is False
