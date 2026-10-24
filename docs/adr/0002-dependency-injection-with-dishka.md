# 2. Dependency injection with Dishka

## Context

Stage 2 introduced three processes (HTTP server, worker, YAML importer) that share some
dependencies and differ in others. The web process must not open replication
connections, the worker does not need HTTP services, and tests need the same services
on top of in-memory storage. Hand-written composition grew into a class with factory
methods that every entrypoint used slightly differently.

## Decision

Use [Dishka](https://github.com/reagento/dishka). Providers in `bootstrap/ioc.py` are
grouped by concern, and each process picks the groups it needs:

| Process  | Providers                                                |
|----------|----------------------------------------------------------|
| web      | `PersistenceProvider`, `ServicesProvider`                |
| worker   | `PersistenceProvider`, `SourceProvider`, `WorkerProvider` |
| importer | `PersistenceProvider`, `ServicesProvider`                |
| tests    | an in-memory provider + `ServicesProvider`               |

Ports are plain `Protocol`s, so factories that are injected (unit of work, sinks) are
also protocols with `__call__` rather than `Callable` aliases: a container needs a real
type to use as a key.

## Consequences

- The graph is validated when a container is built; `tests/unit/test_ioc.py` builds the
  graph of every process without touching a database.
- Resources with a lifecycle (the SQLAlchemy engine) are closed together with the
  container.
- Application and domain code do not import Dishka. Only `bootstrap` and the FastAPI
  adapter (for `FromDishka`) know about it.
