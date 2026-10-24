import pytest

from telescreen.adapters.memory import MemoryDatabase
from telescreen.application.pipelines import (
    CreatePipeline,
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
    PipelineService,
)
from telescreen.domain.errors import InvalidPipelineError, InvalidTransitionError
from telescreen.domain.pipeline import PipelineStatus, SinkSpec, SourceSpec
from telescreen.domain.table import TableName


@pytest.fixture
def database() -> MemoryDatabase:
    return MemoryDatabase()


@pytest.fixture
def service(database: MemoryDatabase) -> PipelineService:
    return PipelineService(database.unit_of_work, frozenset({"kafka"}))


async def test_create_applies_defaults(service: PipelineService, database: MemoryDatabase) -> None:
    pipeline = await service.create(
        CreatePipeline(name="Shop-Main", dsn="env:DSN", tables=["orders", " sales.refunds "])
    )

    assert pipeline.source == SourceSpec("env:DSN", "telescreen_shop_main", "telescreen_shop_main")
    assert pipeline.sink == SinkSpec("kafka", "Shop-Main")
    assert pipeline.tables == (TableName("public", "orders"), TableName("sales", "refunds"))
    assert pipeline.status is PipelineStatus.ACTIVE
    assert list(database.pipelines) == ["Shop-Main"]


async def test_create_rejects_duplicate_name(service: PipelineService) -> None:
    await service.create(CreatePipeline(name="shop", dsn="env:DSN", tables=["orders"]))

    with pytest.raises(PipelineAlreadyExistsError, match="already exists"):
        await service.create(CreatePipeline(name="shop", dsn="env:DSN", tables=["orders"]))


async def test_create_rejects_shared_slot(service: PipelineService) -> None:
    await service.create(CreatePipeline(name="a", dsn="env:DSN", tables=["t"], slot="s"))

    with pytest.raises(PipelineAlreadyExistsError, match="Slot 's'"):
        await service.create(CreatePipeline(name="b", dsn="env:DSN", tables=["t"], slot="s"))


async def test_create_rejects_shared_publication(service: PipelineService) -> None:
    await service.create(CreatePipeline(name="a", dsn="env:DSN", tables=["t"], publication="p"))

    with pytest.raises(PipelineAlreadyExistsError, match="Publication 'p'"):
        await service.create(CreatePipeline(name="b", dsn="env:DSN", tables=["t"], publication="p"))


@pytest.mark.parametrize(
    "command",
    [
        CreatePipeline(name="x", dsn="env:DSN", tables=["t"], sink_kind="carrier-pigeon"),
        CreatePipeline(name="x", dsn="env:DSN", tables=["a.b.c"]),
        CreatePipeline(name="x", dsn="env:DSN", tables=[]),
        CreatePipeline(name="bad name", dsn="env:DSN", tables=["t"]),
    ],
)
async def test_create_rejects_invalid_definitions(
    service: PipelineService, database: MemoryDatabase, command: CreatePipeline
) -> None:
    with pytest.raises(InvalidPipelineError):
        await service.create(command)
    assert database.pipelines == {}


async def test_lifecycle_changes_are_persisted(
    service: PipelineService, database: MemoryDatabase
) -> None:
    await service.create(CreatePipeline(name="shop", dsn="env:DSN", tables=["orders"]))

    await service.pause("shop")
    assert database.pipelines["shop"].status is PipelineStatus.PAUSED

    await service.resume("shop")
    assert database.pipelines["shop"].status is PipelineStatus.ACTIVE

    await service.delete("shop")
    assert database.pipelines["shop"].status is PipelineStatus.DELETING


async def test_invalid_transition_leaves_state_untouched(
    service: PipelineService, database: MemoryDatabase
) -> None:
    await service.create(CreatePipeline(name="shop", dsn="env:DSN", tables=["orders"]))

    with pytest.raises(InvalidTransitionError):
        await service.retry("shop")
    assert database.pipelines["shop"].status is PipelineStatus.ACTIVE


async def test_unknown_pipeline(service: PipelineService) -> None:
    with pytest.raises(PipelineNotFoundError):
        await service.pause("ghost")
