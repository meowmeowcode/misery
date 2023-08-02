from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    Sequence,
    Union,
)

import asyncpg  # type: ignore
import pytest  # type: ignore
from asyncpg import (  # type: ignore
    Connection,
    Pool,
)
from pypika import Table  # type: ignore

from misery import (
    F,
    NotFound,
    Repo,
    TransactionManager,
)

from misery.postgres import (
    PostgresRepo,
    PostgresTransactionManager,
)

from .base import (
    Symptom,
    SymptomType,
    SymptomsRepo,
    Website,
    WebsitesRepo,
)


class SymptomsPostgresRepo(PostgresRepo[Symptom]):
    table = Table("symptoms")


class WebsitesPostgresRepo(PostgresRepo[Website]):
    table = Table("websites")


@pytest.fixture(scope="module")
def conn_str() -> str:
    return "postgresql://misery:misery@localhost/misery"


@pytest.fixture(scope="module", params=[asyncpg.connect, asyncpg.create_pool])
async def conn(request: Any, conn_str: str) -> Union[Connection, Pool]:
    return await request.param(conn_str)


@pytest.fixture(scope="module", autouse=True)
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

    await conn.execute(
        """
            CREATE TABLE websites (
                id integer PRIMARY KEY,
                address text NOT NULL UNIQUE,
                hosts text[],
                framework text
            )
        """
    )

    yield

    await conn.execute(f"drop table symptoms")
    await conn.execute(f"drop table websites")


@pytest.fixture(autouse=True)
async def clean_db(db_schema: None, conn: Connection) -> None:
    await conn.execute(f"TRUNCATE TABLE symptoms")
    await conn.execute(f"TRUNCATE TABLE websites")


@pytest.fixture
def symptoms_repo(conn: Connection) -> SymptomsRepo:
    return SymptomsPostgresRepo(conn)


@pytest.fixture
def websites_repo(conn: Connection) -> WebsitesRepo:
    return WebsitesPostgresRepo(conn)


@pytest.fixture
def transaction_manager(conn: Connection) -> TransactionManager:
    return PostgresTransactionManager(conn)


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
    assert symptoms == [hopelessness, helplessness]


@pytest.mark.parametrize(
    "filters, names",
    [
        ([F.eq("name", "Insomnia")], {"Insomnia"}),
        ([F.neq("name", "Insomnia")], {"Hopelessness", "Helplessness", "Constipation"}),
        ([~F.eq("name", "Insomnia")], {"Hopelessness", "Helplessness", "Constipation"}),
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
        (
            [
                F.or_(
                    F.or_(F.startswith("name", "I"), F.endswith("name", "n")),
                    F.eq("name", "Helplessness"),
                )
            ],
            {"Constipation", "Insomnia", "Helplessness"},
        ),
        (
            [
                (F.startswith("name", "I") | F.endswith("name", "n"))
                | F.eq("name", "Helplessness")
            ],
            {"Constipation", "Insomnia", "Helplessness"},
        ),
        ([F.and_(F.contains("name", "o"), F.contains("name", "ss"))], {"Hopelessness"}),
        (
            [~F.and_(F.contains("name", "o"), F.contains("name", "ss"))],
            {"Insomnia", "Constipation", "Helplessness"},
        ),
        ([F.contains("name", "o") & F.contains("name", "ss")], {"Hopelessness"}),
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


@pytest.mark.parametrize(
    "limit, offset, names",
    [
        (2, 1, ["Helplessness", "Insomnia"]),
        (2, 3, ["Constipation"]),
    ],
)
async def test_get_many_with_limit_and_offset(
    limit: int,
    offset: int,
    names: list[str],
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
    constipation: Symptom,
) -> None:
    symptoms = await symptoms_repo.get_many(limit=limit, offset=offset)
    assert [s.name for s in symptoms] == names


@pytest.mark.parametrize(
    "filters, order, name",
    [
        ([F.startswith("name", "I")], [], "Insomnia"),
        ([F.nstartswith("name", "I")], [], "Helplessness"),
        ([], ["-name"], "Insomnia"),
        ([], ["name"], "Helplessness"),
    ],
)
async def test_get_first(
    filters: Sequence[F],
    order: Sequence[str],
    name: str,
    symptoms_repo: SymptomsRepo,
    helplessness: Symptom,
    insomnia: Symptom,
) -> None:
    symptom = await symptoms_repo.get_first(filters, order)
    assert symptom.name == name


async def test_get_first_not_found(symptoms_repo: SymptomsRepo) -> None:
    with pytest.raises(NotFound):
        await symptoms_repo.get_first()


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


async def test_count_filtered(
    symptoms_repo: SymptomsRepo,
    hopelessness: Symptom,
    helplessness: Symptom,
    insomnia: Symptom,
    constipation: Symptom,
) -> None:
    assert await symptoms_repo.count_filtered(F.gt("id", 2)) == 2


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


@pytest.mark.parametrize(
    "filter_, id_",
    [
        (F.ipin("address", "192.168.1.0/24"), 1),
        (F.nipin("address", "192.168.1.0/24"), 2),
    ],
)
async def test_network_filter(
    filter_: F,
    id_: int,
    website: Website,
    website2: Website,
    websites_repo: WebsitesRepo,
) -> None:
    websites = list(await websites_repo.get_many([filter_]))
    assert [w.id for w in websites] == [id_]


@pytest.mark.parametrize(
    "filter_, ids",
    [
        (F.hasany("hosts", ["test2.com", "something"]), [2]),
        (F.hasany("hosts", ["something"]), []),
    ],
)
async def test_has_any_filter(
    filter_: F,
    ids: int,
    website: Website,
    website2: Website,
    websites_repo: WebsitesRepo,
) -> None:
    websites = list(await websites_repo.get_many([filter_]))
    assert [w.id for w in websites] == ids


@pytest.mark.parametrize(
    "filter_, id_",
    [
        (F.eq("framework", None), 1),
        (F.neq("framework", None), 2),
    ],
)
async def test_null_filter(
    filter_: F,
    id_: int,
    website: Website,
    website2: Website,
    websites_repo: WebsitesRepo,
) -> None:
    websites = list(await websites_repo.get_many([filter_]))
    assert [w.id for w in websites] == [id_]
