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

Wiring is done by hand in `bootstrap` (a composition root) instead of a DI container: the
object graph is small, and explicit construction keeps it easy to follow.

## Consequences

- Use cases are tested with in-memory fakes of the ports.
- Every adapter can be replaced without touching the core (for example, Kafka and NATS
  implement the same `EventSink` port).
- Some boilerplate in `bootstrap` is the price for not having a container.
