# telescreen

> Big Brother is watching your WAL.

Lightweight change data capture for PostgreSQL written in Python. Telescreen reads the
write-ahead log through native logical replication (`pgoutput`), turns row changes into
structured events and publishes them to Kafka or NATS JetStream.

**Status:** work in progress. See [roadmap](#roadmap).

## Roadmap

- [x] Stage 0 — project skeleton, tooling, local infrastructure
- [ ] Stage 1 — `pgoutput` decoder, streaming to Kafka, at-least-once delivery
- [ ] Stage 2 — pipeline metadata in PostgreSQL, HTTP API, status page
- [ ] Stage 3 — initial snapshot with a gap-free handoff to streaming
- [ ] Stage 4 — column filters, PII masking, topic routing, schema tracking
- [ ] Stage 5 — NATS JetStream sink
- [ ] Stage 6 — metrics and integration tests

## Development

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker.

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy
```

Local infrastructure (PostgreSQL with `wal_level=logical`, Kafka in KRaft mode, Kafka UI):

```bash
docker compose up -d
```

| Service    | Address                                   |
|------------|-------------------------------------------|
| PostgreSQL | `localhost:5433` (user/password `postgres`) |
| Kafka      | `localhost:9094`                           |
| Kafka UI   | http://localhost:8081                      |

The PostgreSQL container hosts two databases: `telescreen` for the service's own metadata
and `shop`, a demo source with a few tables to capture.

## License

MIT
