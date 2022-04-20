from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator
from typing import Sequence
from typing import Union

import asyncpg  # type: ignore
import pytest  # type: ignore
from asyncpg import Connection  # type: ignore

from depression.core import Repo
from depression.core import TransactionManager
from depression.pg_repo import PGRepo
from depression.pg_repo import PGTransactionManager
from depression import F

from .base import (
    Symptom,
    SymptomType,
    SymptomsRepo,
)


class SymptomsPGRepo(PGRepo[Symptom]):
    table_name = "symptoms"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def conn_str() -> str:
    return "postgresql://depression:depression@localhost/depression"


@pytest.fixture(scope="session")
async def conn(conn_str: str) -> Connection:
    return await asyncpg.connect(conn_str)


@pytest.fixture(scope="session", autouse=True)
async def db_schema(conn: Connection) -> AsyncGenerator:
    await conn.execute(
        """
        CREATE TABLE symptoms (
            id integer PRIMARY KEY,
            name text NOT NULL UNIQUE,
            type TEXT NOT NULL
        )
    """
    )

    yield

    await conn.execute(f"drop table symptoms")


@pytest.fixture(autouse=True)
async def clean_db(db_schema: None, conn: Connection) -> None:
    await conn.execute(f"TRUNCATE TABLE symptoms")


@pytest.fixture
def symptoms_repo(conn: Connection) -> SymptomsPGRepo:
    return SymptomsPGRepo(conn)


@pytest.fixture
def transaction_manager(conn: Connection) -> PGTransactionManager:
    return PGTransactionManager(conn)


async def test_get(symptoms_repo: SymptomsRepo, hopelessness: Symptom) -> None:
    s = await symptoms_repo.get(id=hopelessness.id)
    assert s == hopelessness


async def test_get_many(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    symptoms = list(await symptoms_repo.get_many())
    assert symptoms == [hopelessness, helplessness]


@pytest.mark.parametrize(
    "filter_, names",
    [
        ([F.eq("name", "Insomnia")], ["Insomnia"]),
        ([F.neq("name", "Insomnia")], ["Hopelessness", "Helplessness", "Constipation"]),
        ([F.contains("name", "les")], ["Hopelessness", "Helplessness"]),
        ([F.contains("name", "LES")], []),
        ([F.ncontains("name", "les")], ["Insomnia", "Constipation"]),
        (
            [F.ncontains("name", "LES")],
            ["Hopelessness", "Helplessness", "Insomnia", "Constipation"],
        ),
        ([F.icontains("name", "LES")], ["Hopelessness", "Helplessness"]),
        ([F.nicontains("name", "LES")], ["Insomnia", "Constipation"]),
        ([F.startswith("name", "H")], ["Hopelessness", "Helplessness"]),
        ([F.startswith("name", "h")], []),
        ([F.nstartswith("name", "H")], ["Insomnia", "Constipation"]),
        (
            [F.nstartswith("name", "h")],
            ["Hopelessness", "Helplessness", "Insomnia", "Constipation"],
        ),
        ([F.istartswith("name", "h")], ["Hopelessness", "Helplessness"]),
        ([F.nistartswith("name", "h")], ["Insomnia", "Constipation"]),
        ([F.endswith("name", "ness")], ["Hopelessness", "Helplessness"]),
        ([F.endswith("name", "NESS")], []),
        ([F.nendswith("name", "ness")], ["Insomnia", "Constipation"]),
        (
            [F.nendswith("name", "NESS")],
            ["Hopelessness", "Helplessness", "Insomnia", "Constipation"],
        ),
        ([F.iendswith("name", "NESS")], ["Hopelessness", "Helplessness"]),
        ([F.niendswith("name", "NESS")], ["Insomnia", "Constipation"]),
        ([F.matches("name", r"^H.*s$")], ["Hopelessness", "Helplessness"]),
        ([F.matches("name", r"^h.*s$")], []),
        ([F.nmatches("name", r"^H.*s$")], ["Insomnia", "Constipation"]),
        (
            [F.nmatches("name", r"^h.*s$")],
            ["Hopelessness", "Helplessness", "Insomnia", "Constipation"],
        ),
        ([F.imatches("name", r"^h.*s$")], ["Hopelessness", "Helplessness"]),
        ([F.nimatches("name", r"^h.*s$")], ["Insomnia", "Constipation"]),
        ([F.in_("name", ("Insomnia", "Constipation"))], ["Insomnia", "Constipation"]),
        (
            [F.nin("name", ("Insomnia", "Constipation"))],
            ["Hopelessness", "Helplessness"],
        ),
        ([F.lt("id", 2)], ["Hopelessness"]),
        ([F.lte("id", 2)], ["Hopelessness", "Helplessness"]),
        ([F.gt("id", 3)], ["Constipation"]),
        ([F.gte("id", 3)], ["Insomnia", "Constipation"]),
    ],
)
async def test_get_many_with_filter(
    filter_: Sequence[F],
    names: list[str],
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
    constipation: Symptom,
) -> None:
    symptoms = await symptoms_repo.get_many(filter_=filter_)
    assert [s.name for s in symptoms] == names


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
):
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
):
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


async def test_clean(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    await symptoms_repo.clean()
    symptoms = list(await symptoms_repo.get_many())
    assert symptoms == []


async def test_count(
    symptoms_repo: SymptomsRepo, hopelessness: Symptom, helplessness: Symptom
) -> None:
    assert await symptoms_repo.count() == 2


async def test_transaction(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    transaction_manager: TransactionManager,
) -> None:
    async with transaction_manager:
        await symptoms_repo.delete(id=hopelessness.id)

    assert await symptoms_repo.exists(id=hopelessness.id) is False


async def test_failed_transaction(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    transaction_manager: TransactionManager,
) -> None:
    try:
        async with transaction_manager:
            await symptoms_repo.delete(id=hopelessness.id)
            raise RuntimeError("failed transaction test")
    except RuntimeError:
        pass

    assert await symptoms_repo.exists(id=hopelessness.id) is True
