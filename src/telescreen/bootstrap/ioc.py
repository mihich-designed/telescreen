"""Dishka providers: the only place that knows which adapter implements which port.

Processes assemble containers from these providers:

- web: ``PersistenceProvider`` + ``ServicesProvider``
- worker: ``PersistenceProvider`` + ``SourceProvider`` + ``WorkerProvider``
- tests swap ``PersistenceProvider`` for in-memory implementations.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from dishka import Provider, Scope, from_context, provide
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from telescreen.adapters.persistence.repositories import SqlUnitOfWork
from telescreen.adapters.persistence.state import SqlCheckpointStore, SqlRuntimeStore
from telescreen.adapters.postgres.source import PostgresReplicationSource
from telescreen.adapters.secrets import EnvSecretResolver
from telescreen.adapters.sinks.factory import SINK_KINDS, BrokerSinkFactory
from telescreen.application.pipelines import PipelineService
from telescreen.application.ports import (
    CheckpointStore,
    ReplicationSource,
    RuntimeStore,
    SecretResolver,
    SinkFactory,
    UnitOfWork,
    UnitOfWorkFactory,
)
from telescreen.application.queries import PipelineQueries
from telescreen.application.runner import PipelineRunner
from telescreen.application.supervisor import Supervisor
from telescreen.bootstrap.settings import Settings


class PersistenceProvider(Provider):
    scope = Scope.APP

    settings = from_context(provides=Settings)

    @provide
    async def engine(self, settings: Settings) -> AsyncIterator[AsyncEngine]:
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        yield engine
        await engine.dispose()

    @provide
    def sessions(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(engine, expire_on_commit=False)

    @provide
    def unit_of_work(self, sessions: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
        def factory() -> UnitOfWork:
            return SqlUnitOfWork(sessions)

        return factory

    checkpoints = provide(SqlCheckpointStore, provides=CheckpointStore)
    runtime = provide(SqlRuntimeStore, provides=RuntimeStore)


class ServicesProvider(Provider):
    scope = Scope.APP

    @provide
    def pipelines(self, uow: UnitOfWorkFactory) -> PipelineService:
        return PipelineService(uow, SINK_KINDS)

    queries = provide(PipelineQueries)


class SourceProvider(Provider):
    scope = Scope.APP

    @provide
    def secrets(self) -> SecretResolver:
        return EnvSecretResolver()

    source = provide(PostgresReplicationSource, provides=ReplicationSource)

    @provide
    def sinks(self, settings: Settings) -> SinkFactory:
        return BrokerSinkFactory(settings.kafka_bootstrap_servers)


class WorkerProvider(Provider):
    scope = Scope.APP

    @provide
    def runner(
        self,
        settings: Settings,
        source: ReplicationSource,
        sinks: SinkFactory,
        checkpoints: CheckpointStore,
        runtime: RuntimeStore,
    ) -> PipelineRunner:
        return PipelineRunner(
            source=source,
            sinks=sinks,
            checkpoints=checkpoints,
            runtime=runtime,
            worker_id=settings.worker_id,
            status_interval=settings.status_interval,
        )

    @provide
    def supervisor(
        self,
        settings: Settings,
        uow: UnitOfWorkFactory,
        runner: PipelineRunner,
        source: ReplicationSource,
        runtime: RuntimeStore,
    ) -> Supervisor:
        return Supervisor(uow, runner, source, runtime, poll_interval=settings.poll_interval)
