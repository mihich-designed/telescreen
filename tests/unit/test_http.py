import asyncio
from collections.abc import Iterator

import pytest
from dishka import Provider, Scope, make_async_container, provide
from fastapi.testclient import TestClient

from telescreen.adapters.http.app import create_app
from telescreen.adapters.http.pages import human_bytes
from telescreen.adapters.memory import MemoryCheckpointStore, MemoryDatabase, MemoryRuntimeStore
from telescreen.application.ports import (
    CheckpointStore,
    RuntimeState,
    RuntimeStatus,
    RuntimeStore,
    UnitOfWorkFactory,
)
from telescreen.bootstrap.ioc import ServicesProvider
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import PipelineStatus, SnapshotMode


class MemoryProvider(Provider):
    scope = Scope.APP

    def __init__(self, database: MemoryDatabase, runtime: MemoryRuntimeStore) -> None:
        super().__init__()
        self.database = database
        self.runtime_store = runtime
        self.checkpoint_store = MemoryCheckpointStore()

    @provide
    def unit_of_work(self) -> UnitOfWorkFactory:
        return self.database.unit_of_work

    @provide
    def runtime(self) -> RuntimeStore:
        return self.runtime_store

    @provide
    def checkpoints(self) -> CheckpointStore:
        return self.checkpoint_store


@pytest.fixture
def database() -> MemoryDatabase:
    return MemoryDatabase()


@pytest.fixture
def runtime() -> MemoryRuntimeStore:
    return MemoryRuntimeStore()


@pytest.fixture
def provider(database: MemoryDatabase, runtime: MemoryRuntimeStore) -> MemoryProvider:
    return MemoryProvider(database, runtime)


@pytest.fixture
def client(provider: MemoryProvider) -> Iterator[TestClient]:
    container = make_async_container(provider, ServicesProvider())
    with TestClient(create_app(container)) as client:
        yield client


def create(client: TestClient, name: str = "shop") -> dict[str, object]:
    response = client.post(
        "/api/pipelines", json={"name": name, "dsn": "env:SHOP_DSN", "tables": ["orders"]}
    )
    assert response.status_code == 201, response.text
    body: dict[str, object] = response.json()
    return body


class TestApi:
    def test_create_and_get(self, client: TestClient) -> None:
        created = create(client)

        assert created["status"] == "active"
        assert created["source"] == {
            "dsn": "env:SHOP_DSN",
            "publication": "telescreen_shop",
            "slot": "telescreen_shop",
        }
        assert created["runtime"] is None
        assert client.get("/api/pipelines/shop").json() == created

    def test_list_includes_runtime_and_checkpoint(
        self, client: TestClient, runtime: MemoryRuntimeStore
    ) -> None:
        create(client)
        runtime.statuses["shop"] = RuntimeStatus("shop", "w1", RuntimeState.STREAMING, 2048)

        [item] = client.get("/api/pipelines").json()

        assert item["runtime"]["state"] == "streaming"
        assert item["runtime"]["lag_bytes"] == 2048

    def test_lifecycle(self, client: TestClient, database: MemoryDatabase) -> None:
        create(client)

        assert client.post("/api/pipelines/shop/pause").json()["status"] == "paused"
        assert client.post("/api/pipelines/shop/resume").json()["status"] == "active"
        response = client.delete("/api/pipelines/shop")

        assert response.status_code == 202
        assert database.pipelines["shop"].status is PipelineStatus.DELETING

    def test_errors(self, client: TestClient) -> None:
        create(client)

        assert client.get("/api/pipelines/ghost").status_code == 404
        assert client.post("/api/pipelines/shop/resume").status_code == 409
        assert client.post("/api/pipelines/shop/explode").status_code == 422
        duplicate = client.post(
            "/api/pipelines", json={"name": "shop", "dsn": "env:X", "tables": ["t"]}
        )
        assert duplicate.status_code == 409
        invalid = client.post(
            "/api/pipelines", json={"name": "bad name", "dsn": "env:X", "tables": ["t"]}
        )
        assert invalid.status_code == 422

    def test_health(self, client: TestClient) -> None:
        assert client.get("/health").json() == {"status": "ok"}


class TestPages:
    def test_dashboard_lists_pipelines(self, client: TestClient) -> None:
        assert "Nothing is being watched yet" in client.get("/").text

        create(client)
        page = client.get("/")

        assert page.status_code == 200
        assert 'href="/pipelines/shop"' in page.text
        assert "shop" in client.get("/partials/pipelines").text

    def test_create_through_form(self, client: TestClient, database: MemoryDatabase) -> None:
        response = client.post(
            "/pipelines",
            data={"name": "shop", "dsn": "env:DSN", "tables": "public.orders\npublic.customers"},
            follow_redirects=False,
        )

        assert response.status_code == 303
        assert response.headers["location"] == "/pipelines/shop"
        assert len(database.pipelines["shop"].tables) == 2

    def test_form_shows_validation_errors(self, client: TestClient) -> None:
        response = client.post(
            "/pipelines", data={"name": "shop", "dsn": "env:DSN", "tables": "a.b.c"}
        )

        assert response.status_code == 422
        assert "Malformed table name" in response.text

    def test_detail_and_actions(self, client: TestClient, database: MemoryDatabase) -> None:
        create(client)

        detail = client.get("/pipelines/shop")
        assert "shop.public.orders" in detail.text
        assert "Pause" in detail.text

        client.post("/pipelines/shop/pause")
        assert database.pipelines["shop"].status is PipelineStatus.PAUSED
        assert "Resume" in client.get("/pipelines/shop").text

    def test_unknown_pipeline_redirects_home(self, client: TestClient) -> None:
        response = client.get("/pipelines/ghost", follow_redirects=False)
        assert response.status_code == 303


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, "—"), (512, "512 B"), (2048, "2.0 KiB"), (5 * 1024**3, "5.0 GiB")],
)
def test_human_bytes(value: int | None, expected: str) -> None:
    assert human_bytes(value) == expected


def test_checkpoint_is_rendered(client: TestClient, provider: MemoryProvider) -> None:
    create(client)
    asyncio.run(provider.checkpoint_store.save("shop", Lsn.parse("0/16B3748")))

    assert client.get("/api/pipelines/shop").json()["checkpoint"] == "0/16B3748"
    assert "0/16B3748" in client.get("/pipelines/shop").text


class TestSnapshotOption:
    def test_api_defaults_to_initial_snapshot(self, client: TestClient) -> None:
        assert create(client)["snapshot"] == {"mode": "initial", "completed": False}

    def test_api_can_skip_snapshot(self, client: TestClient) -> None:
        response = client.post(
            "/api/pipelines",
            json={"name": "shop", "dsn": "env:X", "tables": ["t"], "initial_snapshot": False},
        )

        assert response.json()["snapshot"] == {"mode": "never", "completed": False}

    def test_form_checkbox(self, client: TestClient, database: MemoryDatabase) -> None:
        assert 'name="initial_snapshot" value="true" checked' in client.get("/pipelines/new").text

        client.post("/pipelines", data={"name": "a", "dsn": "env:X", "tables": "t"})
        client.post(
            "/pipelines",
            data={"name": "b", "dsn": "env:X", "tables": "t", "initial_snapshot": "true"},
        )

        assert database.pipelines["a"].snapshot is SnapshotMode.NEVER
        assert database.pipelines["b"].snapshot is SnapshotMode.INITIAL
        assert "pending" in client.get("/pipelines/b").text
        assert "changes only" in client.get("/pipelines/a").text
