# 1. Layered architecture with ports and adapters

## Context

Telescreen talks to several external systems: the source PostgreSQL (replication protocol
and regular SQL), its own metadata database, and one or more message brokers. The core
logic — decoding changes, applying table rules, deciding when a position in the WAL is
safe to acknowledge — must be testable without any of them.

## Decision

The code is split into four layers. Dependencies point inwards only.

| Layer         | Contains                                                         | Depends on            |
|---------------|------------------------------------------------------------------|-----------------------|
| `domain`      | Entities, value objects, invariants. Pure Python, no I/O.        | nothing               |
| `application` | Use cases and the ports (protocols) they need.                   | `domain`              |
| `adapters`    | Implementations of ports: PostgreSQL, Kafka, NATS, HTTP, files.  | `application`, `domain` |
| `bootstrap`   | Settings, wiring, process entrypoints.                           | everything            |

Wiring happens only in `bootstrap`. It started as hand-written construction and moved
to a DI container once there were several processes to assemble, see
[ADR 2](0002-dependency-injection-with-dishka.md).

## Consequences

- Use cases are tested with in-memory fakes of the ports.
- Every adapter can be replaced without touching the core (for example, Kafka and NATS
  implement the same `EventSink` port).
- Infrastructure code lives in `adapters`. SQLAlchemy tables are in
  `adapters/persistence/tables.py`; Alembic migrations sit in `migrations/` at the
  repository root because they are a deployment tool, not part of the running service.
